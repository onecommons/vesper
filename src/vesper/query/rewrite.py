#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
1. convert appropriate expressions not in the where clause (e.g. construct and order by)
   into a join
2. convert the where clause in joins
3. combine any joins that depend on each other into one join
"""
from vesper.query.jqlAST import *
import copy, itertools
from vesper.utils import debugp
from vesper.pjson import ParseContext

class _ParseState(object):

    #: Maps labels to Join op
    labeledjoins = None
    
    def __init__(self, functions, namemap=None):
        self.labeledjoins = {}
        self.labeledjoinorder = []
        self.orphanedJoins = {}
        self._anonJoinCounter = 0
        self.functions = functions
        self.namemap = namemap or {}

    def mapOp(self, op):
        _opmap = {
        "AND" : And,
        "OR" : Or,
        "NOT" : Not,
        "IN" : In,
        "=" : Eq,
        "==" : Eq,
        '!=' : lambda *args: Not(Eq(*args)),
        '<' : lambda *args: Cmp('<',*args),
        '>' : lambda *args: Cmp('>',*args),
        '<=' : lambda *args: Cmp('<=',*args),
        '>=' : lambda *args: Cmp('>=',*args),
        '+' : lambda *args: self.getFuncOp('add',*args),
        '-' : lambda *args: self.getFuncOp('sub',*args),
        '*' : lambda *args: self.getFuncOp('mul',*args),
        '/' : lambda *args: self.getFuncOp('div',*args),
        '%' : lambda *args: self.getFuncOp('mod',*args),
        }
        return _opmap[op]

    def getFuncOp(self, name, *args):
        return self.functions.getOp(name, *args)

    def addLabeledJoin(self, name, join):
        if join.name:
            if join.name != name:
                raise QueryException(
                   "can't assign id %s, join already labeled %s: %s"
                    % (name, join.name, join))
        else:
            join.name = name
        self.labeledjoins.setdefault(name,[]).append(join)

        if name in self.labeledjoinorder:
            #outermost wins
            self.labeledjoinorder.remove(name)
        #assumes this is called in bottoms-up parse order
        self.labeledjoinorder.append(name)

    def nextAnonJoinId(self):
        self._anonJoinCounter += 1
        return '@' + str(self._anonJoinCounter)

    def joinFromConstruct(self, construct, where, groupby, orderby):
        '''
        build a join expression from the construct pattern
        '''
        left = where
        for prop in construct.args:
            if prop != construct.id:
                #don't want treat construct values as boolean filter
                #but we do want to find projections which we need to join
                #(but we skip project(0) -- no reason to join)
                for child in prop.depthfirst(
                 descendPredicate=lambda op: not isinstance(op, (ResourceSetOp, Select))):
                    if isinstance(child, Project):
                        if child.name != '*' and child.fields != [SUBJECT]:
                            if not prop.nameFunc or not child.isDescendentOf(prop.nameFunc):
                                prop.projects.append(child)
                            
                            if prop.ifEmpty == PropShape.omit:
                                child.maybe = True                                              
                            cchild = copy.copy( child )
                            cchild._parent = None
                            cchild.fromConstruct = True
                            if not left:                            
                                left = cchild
                            else:
                                assert child
                                left = And(left, cchild )
                    elif isinstance(child, ResourceSetOp):
                        self.replaceJoinWithLabel(child)
                        #but add the join to left for analysis
                        if not left:
                            left = child
                        else:
                            left = And(left, child)                        
                    elif isinstance(child, AnyFuncOp) and child.isAggregate():
                        construct.hasAggFunc = prop.hasAggFunc = True
                #if isinstance(child, ResourceSetOp):
                #    print '!!!child', child
                #treat ommittable properties as outer joins:
                if prop.ifEmpty == PropShape.omit:
                    for a in prop.args:
                        a.maybe = True
        
        if groupby and not isinstance(groupby.args[0], Label):
            project = copy.copy(groupby.args[0])
            project._parent = None
            assert isinstance(project, Project), 'groupby currently only supports single property name'
            project.fromConstruct = True
            if not left:
                left = project
            else:
                left = And(left, project)
            #hack around constructRefs hack:
            for prop in construct.args:
                if prop.value and prop.value.name == project.name:
                    if getattr(prop.value, 'constructRefs', False):
                        #dont construct groupby property
                        prop.value.constructRefs = False
        
        if orderby:
            for child in orderby.depthfirst():
                if isinstance(child, Project):
                    child.maybe = True                                              
                    cchild = copy.copy( child )
                    cchild._parent = None
                    cchild.fromConstruct = True
                    if not left:                            
                        left = cchild
                    else:
                        assert child
                        left = And(left, cchild )
        

        name = construct.id and construct.id.getLabel() or self.nextAnonJoinId()
        if left:
            left = self.makeJoinExpr(left,name)
            assert left
        else:
            left = Join(name=name)
        self.addLabeledJoin(name, left)

        return left

    logicalops = {
     And : Join,
     Or : Union,
    }

    def _getASTForProject(self, project, name):
        '''
        Return an op that will retrieve the values that match the projection
    
        bar return 
        
        Filter(Eq('bar', Project(PROPERTY)), objectlabel='bar'))
    
        foo.bar returns
    
        jc(Join(
            jc(Filter(Eq('bar', Project(PROPERTY)), objectlabel='bar'), OBJECT),
            jc(Filter(Eq('foo', Project(PROPERTY)) ), SUBJECT)
          ),
        '_1')
    
        JoinCondition(
        Join(
            jc(Filter(None, Eq('bar'), None, subjectlabel='_1'), OBJECT),
            jc(Filter(None, Eq('baz'), None, objectlabel='baz'), OBJECT)
          )
        '_1')
    
        In other words, join the object value of the "bar" filter with object value
        of "baz" filter.
        We add the label'baz' so that the project op can retrieve that value.
        The join condition join this join back into the enclosing join
        using the subject of the "bar" filter.
    
        bar == { ?id where foo }
    
        Filter(Eq('bar'), Join(jc(
            Filter(None, Eq('foo'), None, propertyname='foo'), 'foo'))
    
        ?foo.bar is shorthand for
        { id = ?foo and bar }
        thus:
        Join( Filter(Eq('bar',Project(PROPERTY)), subjectlabel='foo', objectlabel='bar') )
    
        '''
        #XXX we need to disabiguate labels with the same name
        op = None
        if project.name == SUBJECT:
            return None

        for propname in reversed(project.fields):
            #XXX if propname == '*', * == OBJECT? what about foo = * really a no-op
            if not op:
                op = Filter(Eq(PropString(propname), Project(PROPERTY)),
                                                    objectlabel=propname)
                op.fromConstruct = project.fromConstruct
            else:
                subjectlabel = self.nextAnonJoinId()
                filter = Filter(Eq(PropString(propname), Project(PROPERTY)),
                            objectlabel=propname, subjectlabel=subjectlabel)
                filter.fromConstruct = project.fromConstruct
                #create a new join, joining the object of this filter with
                #the subject of the prior one
                op = JoinConditionOp(
                        Join( JoinConditionOp(op, SUBJECT),
                            JoinConditionOp(filter, OBJECT)), subjectlabel)
        
        if project.varref and project.varref != name:
            #project is on a different join
            op.addLabel(project.varref)
            op = Join(op)
            self.addLabeledJoin(project.varref, op)
            return None
        else:
            return op

    def replaceJoinWithLabel(self, child):
        if not child.name:
            child.name = self.nextAnonJoinId()
            self.addLabeledJoin(child.name, child)
        #replace this join with a Label
        newchild = Label(child.name)
        newchild.maybe = child.maybe
        if child.parent:
            child.parent.replaceArg(child,newchild)
        return newchild
    
    def makeJoinExpr(self, expr, name):
        '''
        Rewrite expression into Filters, operations that filter rows,
        and ResourceSetOps (Join, Union, Except), which group together the Filter
        results by id (primary key).
        
        We also need to make sure that filter which apply individual statements
        (id, property, value) triples appear before filters that apply to more than
        one statement and so operate on the simple filter results.

        We only translate operations that dependent on the join, we leave the rest alone.
        '''
        cmproots = []
        to_visit = []
        visited = set()
        to_visit.append( (None, expr) )
    
        newexpr = None
        while to_visit:
            parent, v = to_visit.pop()
            if id(v) not in visited:
                visited.add( id(v) )
                                                
                notcount = 0
                while isinstance(v, Not):
                    notcount += 1
                    assert len(v.args) == 1
                    v = v.args[0]
                
                #map And and Or to Join and Union
                optype = self.logicalops.get(type(v))
                if optype:
                    if notcount % 2: #odd # of nots
                    #if the child of the Not is a logical op, we need to treat this
                    #as a Except op otherwise just include it in the compare operation
                        notOp = Except()
                        if not parent:
                            parent = newexpr = notOp
                        else:
                            parent.appendArg(notOp)
                            parent = notOp
    
                    if not parent:
                        parent = newexpr = optype(name=name)
                    elif type(parent) != optype:
                        #skip patterns like: and(and()) or(or())
                        newop = optype()
                        parent.appendArg(newop)
                        parent = newop

                    #XXX comparison like a = b = c are currently not allowed
                    #but if they are, distribute ops so that a = b and b = c
                    to_visit.extend([(parent, a) for a in v.args]) #descend
                else: #if not isinstance(v, Join): #joins has already been processed
                    if not parent: #default to Join
                        parent = newexpr = Join(name=name)
                    if notcount % 2:
                        v = Not(v)
                    cmproots.append( (parent, v) )
    
        #for each top-level comparison in the expression
        #parent is a join and root will be an immediate child of the join
        for parent, root in cmproots:
            #first add filter or join conditions that correspond to the columnrefs
            #(projections) that appear in the expression

            joinType = JoinConditionOp.INNER
    
            #look for Project ops but don't descend into ResourceSetOp (Join) ops
            projectops = []
            skipRoot = False
            labels ={}
            for child in root.depthfirst(
                    descendPredicate=lambda op: not isinstance(op, ResourceSetOp)):
                if isinstance(child, ResourceSetOp):                
                    #print 'child', child, 'isroot', child is root
                    if child is root:                        
                        #e.g. "where( {...} )" or "... and {...}"
                        #don't include as a filter in parent join
                        #XXX this should replace this with a label
                        #instead of setting skipRoot = True
                        #i.e. root = self.replaceJoinWithLabel(child)
                        #but then we need to deal with Filter(label)
                        #which we don't yet handle well
                        child._parent = None
                        skipRoot = True
                    else:
                        self.replaceJoinWithLabel(child)
                    #print 'adding orphan', child, 'to', parent
                    self.orphanedJoins.setdefault(parent,[]).append(child)
                elif isinstance(child, Project):
                    projectop = self._getASTForProject(child, name)
                    if projectop:
                        projectops.append( (child, projectop) )
                    if child is root:
                        #don't include bare Project as filter, projectops should take of that
                        skipRoot = True
                        #XXX enable this but we need to implement EXIST() 
                        #and joinFromConstruct use exists instead of a raw Project
                        #because we don't want to filter out False values
                        #if isinstance(projectop, Filter):
                            #this was bare reference to a property field
                            #we want this to only include rows when the property value evaluates to true
                        #    assert len(child.fields) == 1 and not child.varref
                        #    projectop.appendArg(self.getFuncOp('bool', Project(PROPERTY) ))
                    elif child.parent is root and isinstance(root, Not):
                        #a filter like "not prop"
                        joinType = JoinConditionOp.ANTI
                        skipRoot = True
                #elif isinstance(child, Label):
                #    labels.setdefault(child.name,[]).append(child)
                if child.maybe:
                    if child is root or isinstance(child, (Project, Label, ResourceSetOp)):
                        joinType = JoinConditionOp.LEFTOUTER
                    else:
                        raise QueryException('illegal maybe expression: %s' % child)

            #try to consolidate the projection filters into root filter.
            if not skipRoot:
                filter = Filter(root)
                consolidateFilter(filter, projectops)

            for (project, projectop) in projectops:
                if isinstance(projectop, JoinConditionOp):
                    projectop.join=joinType
                    parent.appendArg(projectop)
                else:
                    parent.appendArg(JoinConditionOp(projectop, join=joinType))

            if not skipRoot:
                parent.appendArg( JoinConditionOp(filter, join=joinType) )
    
        #XXX remove no-op and redundant filters
        assert newexpr
        validateTree(newexpr)
        return newexpr

    def prepareJoinMove(self, join):
        if isinstance(join.parent, Select):
            if not join.name:
                join.name = self.nextAnonJoinId()
            join.parent.construct.id.appendArg( Label(join.name) )

    def _joinLabeledJoins(self):
        '''
        Combine joins that share the same join label
        '''
        labeledjoins = {}
        #sort labels by the order in which they appear in labeledjoinorder list
        for label, joins in sorted(self.labeledjoins.items(),
                     key=lambda a: self.labeledjoinorder.index(a[0]) ):
            if not joins or not label: #don't combine unlabeled joins
                continue
            #construct that only have id labels will not have a join
            #we only want to add the join if there are no another joins for the label
            firstjoin = None
            for join in joins:
                if join.parent:
                    firstjoin = join
                    break
            if not firstjoin:
                firstjoin = joins[0]
            
            for join in joins:
                if firstjoin:
                    continue
                self.prepareJoinMove(join)
                for child in join.args:
                    firstjoin.appendArg(child)
                join.parent = None

            labeledjoins[label] = firstjoin
            firstjoin.name = label
        return labeledjoins

    def _findJoinsInDocOrder(self, root, joinsInDocOrder):
        for child in root.depthfirst():
            if isinstance(child, ResourceSetOp):
                joinsInDocOrder.append(child)
                for orphan in self.orphanedJoins.pop(child,[]):
                    self._findJoinsInDocOrder(orphan, joinsInDocOrder)

    def _findJoinAliases(self, join, preds):
        remainingPreds = {}
        joinPreds = []
        aliasingPreds = []
        aliases = set([join.name])
        aliasCount = 0
        #repeat to handle cases like "id = ?foo and ?foo = ?bar"
        while aliasCount < len(aliases): #an alias was added
            aliasCount = len(aliases)
            #assert all(label.parent for (pred, label) in preds)
            for pred, label in preds:
                if not label.parent:
                    continue #already handled
                handled = False
                simpleEq = label.parent is pred and isinstance(pred, Eq)
                #XXX handle In op like Eq
                if simpleEq:
                    def addAlias(alias):
                        aliases.add(alias)
                        aliasingPreds.append( (pred, label, pred.parent) )
                        #remove the filter
                        assert join is pred.parent.parent.parent, pred.parent.parent.parent
                        assert len(pred.parent.args) == 1, pred.parent
                        join.removeArg(pred.parent.parent)
                        handled = True

                    otherside = label is pred.left and pred.right or pred.left
                    if isinstance(otherside, Project):
                        if otherside.name == SUBJECT:
                            #e.g. id = ?label
                            #XXX handle varref, e.g. ?foo.id
                            addAlias(label.name)
                        else:#if otherside.name == OBJECT or otherside.varref in aliases:
                            #e.g. prop = ?label
                            filter = pred.parent
                            assert isinstance(filter, Filter), filter
                            joinPreds.append( (pred, label, filter) )
                            filter.removeArg(pred)
                            handled = True
                    elif isinstance(otherside, Label):
                        if otherside.name in aliases:
                            #e.g. ?label = ?self
                            #the other label refers to this join
                            #we can infer this label does also
                            addAlias(label.name)
                        elif label.name in aliases:
                            #?self = ?label, we can infer the other label
                            #refers to this join also
                            addAlias(otherside.name)
                        #else: #e.g. ?foo = ?bar both sides refer other joins

                if not handled:
                    if label.name in aliases:
                        #if self, replace with subject
                        label.parent.replaceArg(label, Project(SUBJECT))
                    else:
                        remainingPreds[pred] = label

            #remove from joinPreds and remainingPreds any preds that a subsequent
            #pass figured out was an alias to itself
            for pred, label, filter in aliasingPreds:
                if (pred, label, filter) in joinPreds:
                    joinPreds.remove( (pred, label, filter) )

                remainingPreds.pop(pred, None)

            #print 'analyzepreds', aliases, joinPreds, remainingPreds
            depPreds = [pred for pred, label in remainingPreds.items() 
                         if not pred.isIndependent()]
            #not join with correct join:
            #or type(pred) == Eq and label.parent is pred and otherside.isIndependent()
            if depPreds:
                raise QueryException("only equijoins currently supported: %s" % depPreds)
        return aliases, joinPreds, remainingPreds

    def _analyzeJoinPreds(self, join, preds):
        pass #XXX

    def _getJoinPreds(self, joins):
        '''
        Return a dictionary mapping joins to a set of alias names and a list of 
          join predicate expressions that reference that join
        '''
        #any filter expression with a label or project with a varref is candidate
        #for a join condition.
        #first, look for expessions that just indicate equivalency between join objects
        #e.g. ?foo = ?bar or ?foo.id = id and treat those equivalent labels as aliases
        #second, simple equi-joins where one side is a label or subject project of the current join
        #and the other is an expression that depends on one other join
        #finally collect the remaining expressions that depend on more than one join
        #these will be joined together as cross-joins so the filter has to both row sets
        predMap = {}
        for join in joins:
            preds = []
            #find the expressions in each that have a label reference
            for label in join.depthfirst(
              descendPredicate=lambda op: op is join or not isinstance(op, ResourceSetOp)):
                
                if not isinstance(label, Label):
                    continue
                assert label.parent
                pred = None
                parent = label
                while parent:
                    if isinstance(parent.parent, Filter):
                        pred = parent
                        break
                    parent = parent.parent
                assert pred
                preds.append((pred, label))
            #look at the expessions to find ones which indicate equivalency
            #between labels (aliases) or are equijoins (joinPreds)
            aliases, joinPreds, remainingPreds = self._findJoinAliases(join, preds)
            predMap[join] = (aliases, joinPreds)
        return predMap

    def _makeJoin(self, followingJoins, joinReferences, labels, aliases, joinPreds):
        """
        Join refs
        we want to build joins deepest first so that the top level join stays
        at top.

        Any joins with the same name we join together
        Join(..., 'foo') + Join(..., 'foo') => Join(..., Join(...) )

        Any joins that reference that join are joined with the join predicate
        set to the predicate used by the label reference

        Join(...,'foo') + Join(Filter('prop' = ?foo)) => Join(...,
                                               Join(Filter('prop')) on 'prop')
        """
        join = followingJoins.pop(0)

        def getTopJoin(join, top):
            candidate = parent = join
            while parent:
                if isinstance(parent, ResourceSetOp):
                    if parent is top:
                        return candidate
                    candidate = parent
                parent = parent.parent
            return candidate

        for joinPred, label, filter in joinPreds:
            refname = label.name
            refjoin = labels.get(refname)
            if not refjoin:
                raise QueryException("reference to unknown label: %r" % refname)
                #XXX
                #print 'unreferenced label', refname, filter, joinPred
                #assert isinstance(joinPred, Eq)
                #otherside = label is joinPred.left and joinPred.right or joinPred.left
                #assert isinstance(otherside, Project)
                #filter.appendArg( Eq(Project(PROPERTY), PropString(otherside.name)))
                #filter.addLabel(otherside.name, OBJECT)
                #assert joinPred.parent is None
                continue                
            if refjoin not in followingJoins:
                #we only want to handle included joins
                joinReferences.append( (label, joinPred, filter, join) )
                continue    
            topjoin = getTopJoin(refjoin, join)
            propname = flatten( (name for name, pos in filter.labels if pos == OBJECT) )
            assert isinstance(filter.parent, JoinConditionOp)
            jointype = filter.parent.join
            self.prepareJoinMove(topjoin)
            filterPred = Eq(Project(PROPERTY), PropString(propname))#XXX use joinPred?
            if topjoin.args:
                topjoin.appendArg(
                        JoinConditionOp(
                          Filter(filterPred, subjectlabel=propname+'#id',
                                 objectlabel=propname),
                          position=OBJECT, join=jointype)
                        )
                tmpJoin = topjoin
                #tmpJoin = Join(
                #   JoinConditionOp(
                #     Filter(filterPred, subjectlabel=propname+'#id',
                #            objectlabel=propname),
                #     position=OBJECT, join=jointype),
                #   topjoin
                #)
                join.appendArg( JoinConditionOp(tmpJoin,
                    position=propname+'#id', join = jointype
                ) )
                filter.parent = None
            else:
                filter.addLabel(topjoin.name, OBJECT)
                topjoin.parent = None
            #remove pred from Filter
            joinPred.parent = None #XXX already done, no?

        #see if any of the joins we've encountered so far had a reference to the
        #current join
        for label, pred, filter, refjoin in joinReferences:
            if label.name in aliases:
                #reference to current join, join now
                topjoin = getTopJoin(refjoin, join)
                if topjoin not in followingJoins:
                    continue                
                assert isinstance(filter.parent, JoinConditionOp)
                jointype = filter.parent.join
                self.prepareJoinMove(topjoin)
                propname = flatten( (name for name, pos in filter.labels if pos == OBJECT) )                
                join.appendArg( JoinConditionOp(topjoin, propname, jointype) )
                #remove pred from Filter
                pred.parent = None

    def buildJoins(self, root):
        #first join together any joins that share the same name:
        validateTree(root)
        #combine joins that have the same label:
        labeledjoins = self._joinLabeledJoins()
        validateTree(root)
        joinsInDocOrder = []
        self._findJoinsInDocOrder(root, joinsInDocOrder)
        assert not self.orphanedJoins, 'orphaned joins left-over: %s' % self.orphanedJoins

        #build a mapping between joins and predicates that each join participates in
        joinPredicates = self._getJoinPreds(joinsInDocOrder)

        #next, in reverse document order (i.e. start with the most nested joins)
        #join together joins that reference each other
        refs = []
        validateTree(*joinsInDocOrder)
        labels = {}
        for k, v in joinPredicates.items():
            for alias in v[0]:
                labels[alias] = k
        for i in xrange(len(joinsInDocOrder)-1, -1, -1):
            aliases, joinPreds = joinPredicates[joinsInDocOrder[i]]
            self._makeJoin(joinsInDocOrder[i:], refs, labels, aliases, joinPreds)

        #finally, make any non-empty labeled joins that we didn't merge into another
        #query a cross-join
        #print 'labeledjoins', labeledjoins 
        for k,v in labeledjoins.items():
            if not v.parent and v.args:
                if not root.where:
                    root.appendArg(v)
                else:
                    root.where.appendArg( JoinConditionOp(v, k, 'x') )
        assert all(join.parent or not join.args for join in joinsInDocOrder)

def consolidateFilter(filter, projections):
    '''
    Crucial optimization is to consolidate filters that can be applied at once:
    
    e.g. instead foo = 'bar' being:


    Filter(Eq(Project(PROPERTY), 'foo'), objectlabel='foo')
    Filter(Eq(Project('foo'),'bar'))

    consolidate those into:

    Filter(Eq(Project(PROPERTY), 'foo'),Eq(Project(OBJECT), 'bar'), objectlabel='foo')

    A consolidated filter can only have predicate per position

    e.g. foo = bar

    goes from:

    Filter(Eq(Project(PROPERTY), 'foo'), objectlabel='foo')
    Filter(Eq(Project(PROPERTY), 'bar'), objectlabel='bar')
    Filter(Eq(Project('foo'),Project('bar')))

    only can be:

    Filter(Eq(Project(PROPERTY), 'bar'), objectlabel='bar')
    Filter(Eq(Project(PROPERTY), 'foo'),Eq(Project(OBJECT), Project('bar')) )

    however, this isn't much of an optimization, so, for simplicity, we don't bother
    '''
    #XXX consolidate subject filters
    filterprojects = [(p, i) for (i, (p,f)) in enumerate(projections)
                                                if isinstance(f, Filter)]
    if len(filterprojects) == 1:
        p, i = filterprojects[0]
        name = p.name
        assert len(p.fields) == 1
        p.fields = [OBJECT] #replace label with pos
        filter.appendArg( Eq(Project(PROPERTY), PropString(name)) )
        filter.addLabel(name, OBJECT)
        #remove replaced filter:
        del projections[i]
        return True
    return False

def removeDuplicateConstructFilters(root):
    '''
    find joinconditions marked as "bare project from construct" and see if there are
    '''
    for join in root.depthfirst(
      descendPredicate=lambda op: not isinstance(op, ResourceSetOp)):
        if isinstance(join, ResourceSetOp):
            constructFilters = {}
            filterProjects = [] 
            for jc in join.args:
                assert isinstance(jc, JoinConditionOp)
                child = jc.op
                if isinstance(child, ResourceSetOp):
                    removeDuplicateConstructFilters(child)
                    continue
                assert isinstance(child, Filter)
                propertyName = None
                for label, pos in child.labels:
                    if pos == OBJECT:                        
                        propertyName = label
                        break
                if not propertyName:
                    continue                
                if child.fromConstruct:
                    constructFilters.setdefault(propertyName, []).append(jc)
                else:
                    filterProjects.append(propertyName)
            
            #print 'constructProjects', constructFilters, 'filterProjects', filterProjects
            #remove filters generated by construct expressions that match 
            #where-clause filters
            for propertyName in filterProjects:
                constructJCs = constructFilters.get(propertyName)
                while constructJCs:
                    jc = constructJCs.pop()
                    jc.parent.removeArg(jc)
            
            #look at the left-over construct filters and remove any duplicates
            #per property name
            for constructJCs in constructFilters.values():
                while len(constructJCs) > 1:
                    jc = constructJCs.pop()
                    jc.parent.removeArg(jc)