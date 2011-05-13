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
from vesper.utils import debugp, getTransitiveClosure
from vesper.pjson import ParseContext
from vesper.backports import all

class _ParseState(object):

    #: Maps labels to Join op
    labeledjoins = None
    
    def __init__(self, functions, namemap=None):
        self.labeledjoins = {}
        self.labeledjoinorder = []
        self.orphanedJoins = {}
        self._anonJoinCounter = 0
        self.functions = functions
        self.namemap = namemap

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
            #outermost wins (this assumes we're calling addLabeledJoin during bottom-up parsing)
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

    def _getASTForProject(self, project, name, parentJoin):
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
                op = Filter(Eq(PropString(propname), Project(PROPERTY, maybe=project.maybe)),
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
            self.orphanedJoins.setdefault(parentJoin,[]).append(op)
            return None
        else:
            return op

    def replaceJoinWithLabel(self, child):
        if not child.name:
            child.name = self.nextAnonJoinId()
            self.addLabeledJoin(child.name, child)
        #replace this join with a Label
        newchild = Label(child.name, maybe = child.maybe)
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
            dependentVarCount = 0
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
                        dependentVarCount += 1
                    #print 'adding orphan', child, 'to', parent
                    self.orphanedJoins.setdefault(parent,[]).append(child)
                elif isinstance(child, Project):
                    dependentVarCount += 1
                    projectop = self._getASTForProject(child, name, parent)
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
                elif isinstance(child, Label):
                    dependentVarCount += 1

                if child.maybe:
                    if child is not root and not isinstance(child, (Project,)):
                        raise QueryException('illegal maybe expression', child)

            #try to consolidate the projection filters into root filter.
            if not skipRoot:
                filter = Filter(root)
                if dependentVarCount > 1:
                    filter.complexPredicates = True
                    #complexPredicates can't have their names replaced with positions
                    #so don't call consolidateFilter() on them
                else:
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
        #if the parent of the join we are about to move is a Select
        #make sure the select construct still references that join by its label
        if isinstance(join.parent, Select):
            if not join.name:
                join.name = self.nextAnonJoinId()
            join.parent.construct.id.appendArg( Label(join.name) )
            return True
        return False

    def _joinLabeledJoins(self):
        '''
        Combine joins that share the same join label
        '''
        consolidatedjoins = []
        removed = []
        #sort labels by the order in which they appear in labeledjoinorder list
        for label, joins in sorted(self.labeledjoins.items(),
                     key=lambda a: self.labeledjoinorder.index(a[0]) ):
            if not joins:
                continue

            #if not label: continue #don't combine unlabeled joins
            assert label, 'every join should have a label'

            #the firstjoin (the topmost join) needs to be the first one with a parent
            firstjoin = None
            for join in joins:
                if join.parent:
                    firstjoin = join
                    break
            if not firstjoin:
                firstjoin = joins[0]

            for join in joins:
                if join is firstjoin:
                    continue
                self.prepareJoinMove(join)
                for child in join.args:
                    firstjoin.appendArg(child)
                join.parent = None
                removed.append(join)

            consolidatedjoins.append(firstjoin)
            firstjoin.name = label
        return consolidatedjoins, removed

    def _findJoinsInDocOrder(self, root, joinsInDocOrder, removed):
        for child in root.depthfirst():
            if isinstance(child, ResourceSetOp):
                joinsInDocOrder.append(child)
                for orphan in self.orphanedJoins.pop(child,[]):
                    if orphan in removed:
                        continue
                    self._findJoinsInDocOrder(orphan, joinsInDocOrder, removed)

    def buildJoins(self, root):
        #combine joins that have the same label:
        joins, removedJoins = self._joinLabeledJoins()
        validateTree(root)
        joinsInDocOrder = []
        self._findJoinsInDocOrder(root, joinsInDocOrder, removedJoins)
        assert not self.orphanedJoins, 'orphaned joins left-over: %s' % self.orphanedJoins
        assert set(joinsInDocOrder) == set(joins), 'missing join in doc: %s' % (
                                              set(joins) - set(joinsInDocOrder))

        joinsInDocOrder, simpleJoins, complexJoins = self._findJoinPreds(root, joinsInDocOrder)
        #next, in reverse document order (i.e. start with the most nested joins)
        #join together simpleJoinPreds that reference each other
        #need to follow this order to ensure that the outermost joins are on the
        #left-side otherwise right outer joins (the "maybe" operator) will break

        validateTree(*joinsInDocOrder)
        joinFromLabel = {}
        for join in joinsInDocOrder:
            joinFromLabel[join.name] = join
        for i in xrange(len(joinsInDocOrder)-1, -1, -1):
            simpleJoins = self._makeSimpleJoins(joinsInDocOrder[i:],
                                            joinFromLabel, simpleJoins)
        assert not simpleJoins        

        #now join together complexJoins as crossjoins 
        #and set the filter as a complex predicate                
        for filter, refs, ismaybe in complexJoins:
            if ismaybe:
                raise QueryException(
                "MAYBE operation not supported on complex join predicates")            
            filter.complexPredicates = True
            #find parent join to join with: give preference to one 
            #that already joined with another join
            parentjoin = None
            otherjoins = []
            for name in refs:
                candidate = joinFromLabel.get(name)
                if not candidate:
                    raise QueryException(
                        "unreferenced label '%s' in complex join" % name)
                if candidate.parent and not isinstance(candidate.parent, Select):
                    candidate = getTopJoin(candidate, root.where)
                    if parentjoin and parentjoin is not candidate:
                        raise QueryException("Not supported: complex join that"
                            "joins more than one join that is already joined")
                    else:
                        parentjoin = candidate
                elif candidate is root.where:
                    parentjoin = candidate
                else:
                    otherjoins.append(candidate)

            for join in otherjoins:
                if not parentjoin:                                        
                    if root.where:
                        parentjoin = root.where
                    else:
                        if filter.parent.parent is not join:
                            join.appendArg(filter) #move filter to parentjoin
                        root.appendArg(join)                
                if parentjoin:
                    assert parentjoin is not join, (join.name, root)
                    parentjoin.appendArg( JoinConditionOp(join, filter, 'x') )
                                                
        #next, make any non-empty labeled joins that we haven't yet merged
        #into another join a cross-join
        for join in joinsInDocOrder:
            if join.maybe:
                raise QueryException(
                    "MAYBE operation not supported on uncorrelated filter sets")
            if not join.parent and join.args:
                if not root.where:
                    root.appendArg(join)
                else:
                    root.where.appendArg( JoinConditionOp(join, join.name, 'x') )
        assert all(join.parent or not join.args for join in joinsInDocOrder)

        #finally, removed empty joins from nested selects
        for join in joinsInDocOrder:
            if isinstance(join.parent, Select) and join.parent.parent and not join.args:
                join.parent = None

    def _findJoinPreds(self, root, joins):
        joinsByName = {}
        simpleJoinCandidates = []; complexJoinCandidates = []
        aliases = {}
        for join in joins:
            #for each filter
            #if Eq and both sides are simple references (?label or id or ?ref.id)
            #add to aliases and remove filter from join (we'll join together later)
            aliases.setdefault(join.name, [])
            joinsByName[join.name] = join
            for filter in join.depthfirst(
              descendPredicate=lambda op: op is join or not isinstance(op, ResourceSetOp)):
                if not isinstance(filter, Filter):
                    continue
                for arg in filter.args:                    
                    if isinstance(arg, Eq):
                        leftname, leftprop = getNameIfSimpleJoinRef(
                                                arg.left, join.name, filter)
                        rightname, rightprop = getNameIfSimpleJoinRef(
                                                arg.right, join.name, filter)
                        #if both sides are either a project or label
                        if leftname and rightname:                             
                            #its an alias
                            if not leftprop and not rightprop: 
                                #None or 0 (SUBJECT)
                                #its an alias
                                if arg.maybe:
                                    raise QueryException("maybe on an aliasing join not allowed", arg)
                                if leftname != rightname:
                                    aliases.setdefault(leftname, []).append(
                                            rightname)
                                    aliases.setdefault(rightname, []).append(
                                            leftname)
                                #remove this predicate from the filter
                                arg.parent = None
                                continue
                            else:
                                #expressions like ?foo = ?bar.prop or ?a.prop = ?b.prop
                                candidate = (join.name, arg, leftname, leftprop,
                                                rightname, rightprop)
                                simpleJoinCandidates.append(candidate)
                                continue
                        #elif leftname or rightname:
                            #XXX support cases where one side is a complex expression
                    complexJoinCandidates.append(filter)
                if not filter.args:
                    #we must have removed all its predicates so
                    #remove the filter and its join condition
                    join.removeArg(filter.parent)

        #combine together joins that are just aliases of each other
        #assumes join list is in doc order so taht nested joins gets subsumed by outermost join
        aliases = getTransitiveClosure(aliases)
        renamed = {}
        removed = []
        for j in joins:
            for name in aliases[j.name]:
                if name == j.name:
                    continue
                renamed[name] = j.name 
                ja = joinsByName.get(name)
                if not ja:
                    continue
                for child in ja.args:
                    j.appendArg(child)
                ja.name = j.name
                self.prepareJoinMove(ja)
                ja.parent = None
                removed.append(ja)
        joins = [j for j in joins if j not in removed]

        import itertools
        for join in itertools.chain((root,), joins):
            for child in join.depthfirst(
              descendPredicate=lambda op: 
                        op is join or not isinstance(op, ResourceSetOp)):
                if isinstance(child, (Label,Join)):
                    if child.name in renamed:
                        child.name = renamed[child.name]
                elif isinstance(child, Project) and child.varref:
                    if child.varref in renamed:
                        child.varref = renamed[child.varref]

        assert len(set([j.name for j in joins])) == len(joins), 'join names not unique'

        #now that we figured out all the aliases, we can look for join predicates
        simpleJoins = []
        for joinname, pred, leftname, leftprop, rightname, rightprop in simpleJoinCandidates:
            leftname = renamed.get(leftname,leftname)
            rightname = renamed.get(rightname,rightname)
            filter = pred.parent
            if leftname == rightname:
                #both references point to same join, so not a join predicate after all
                if pred.maybe:
                    raise QueryException(
         'MAYBE can not be used on a filter that is not a join condition', pred)
                if leftprop == rightprop:
                    #identity (a=a), so remove predicate
                    #XXX add user warning
                    pred.parent = None
                    if not filter.args:
                        filter.parent.parent = None
                else:
                    #expression operates on more than one project, need to
                    #execute after both Projects have been retreived
                    filter.complexPredicates = True
            else:
                simpleJoins.append((leftname, leftprop, rightname, rightprop, filter, pred.maybe))
                #pred is just a Projects so its already handled by another 
                #filter predicate (see makeJoinExpr() when skipRoot = True)
                #, so remove this one
                if not pred.siblings:
                    filter.parent.parent = None
                else:
                    pred.parent = None
        
        #xxx handle case like { ?bar ?foo.prop = func() or func(?foo) }
        #aren't these complex (cross) joins but rather misplaced filters
        #that belong in the ?foo join?

        #check if the remaining filter predicates are complex joins
        complexJoins = []
        projectPreds = []
        
        for filter in complexJoinCandidates:
            if not filter.parent:
                continue
            join = filter.parent.parent
            #if the filter predicates have reference to another join, its a complex join
            #and while we're at it, collect Project predicates for maybe analysis and fixup
            for pred in filter.args:
                joinrefs = {}
                for label in pred.depthfirst():
                    #check if the filter has reference to a different join
                    if isinstance(label, Label):
                        joinrefs.setdefault(label.name, []).append(label)
                        if label.name == join.name:
                            #to reduce the number of equivalent ops
                            #replace this with Project(SUBJECT)
                            newchild = Project(SUBJECT, label.name,
                                                maybe=label.maybe)
                            label.parent.replaceArg(label,newchild)
                    elif isinstance(label, Project):
                        if not label.varref:
                            label.varref = join.name
                        joinrefs.setdefault(label.varref, []).append(label)
                        
                        propertyname = None
                        if isinstance(label.name, int):
                            if label.name == PROPERTY:
                                propertyname = filter.labelFromPosition(OBJECT)
                        else:
                            propertyname = label.name                        
                        if propertyname:
                            projectPreds.append( (label.varref or join.name,
                                              propertyname, label.maybe, pred) )

                if len(joinrefs) == 2 and isinstance(pred, Eq):                        
                    (leftname, leftops), (rightname, rightops) = joinrefs.items()
                    simple = self._makeHalfSimpleJoin(pred, join.name, leftname,
                      leftops, rightname)
                    if not simple: #try reverse order
                        simple = self._makeHalfSimpleJoin(pred, join.name,
                          rightname, rightops, leftname)
                          
                    if simple:
                        simpleJoins.append( simple )
                    else:
                        complexJoins.append( (filter,                                                       
                                set([leftname,rightname]), pred.maybe) )
                elif len(joinrefs) > 1:
                    complexJoins.append((filter,set(joinrefs.keys()),pred.maybe))
                else:                    
                    if pred.maybe and not isinstance(pred, Project):
                        raise QueryException(
         'MAYBE can not be used on a filter that is not a join condition', pred)
                    #if there's only 1 joinref but it not referencing the join
                    #that its part of, move the filter to the join that is referencing
                    if len(joinrefs) == 1 and iter(joinrefs).next() != join.name:
                        #XXX implement this
                        raise QueryException(
        'Filters that refer to a different filter set are not yet implemented.')

        _fixMaybeFilters(projectPreds)

        return joins, simpleJoins, complexJoins

    def _makeSimpleJoins(self, followingJoins, joinFromLabel, simpleJoins):
        """
        """
        #find the simplepreds that reference this join
        unhandledJoins = []
        join = followingJoins.pop(0)
        joinname = join.name
        for joinInfo in simpleJoins:
            leftname, leftprop, rightname, rightprop, filter, maybe = joinInfo
            if leftname == joinname:
                refname = rightname
            elif rightname == joinname:
                refname = leftname
                #reverse them
                leftprop, rightprop = rightprop, leftprop
            else:
                unhandledJoins.append( joinInfo )
                continue #no references to this join

            def addLabel(refname):
                '''
                just add object label to filter with prop
                '''
                for joincond in join.args:
                    #find the filter that has the Project for the leftprop
                    if (isinstance(joincond.op, Filter)
                        and joincond.op.labelFromPosition(OBJECT) == leftprop):
                        joincond.op.addLabel(refname, OBJECT)
                        if maybe:
                            assert joincond.join in ['i', 'l']
                            joincond.join = 'l'
                        return True
                return False

            if refname not in joinFromLabel: #just add object label to filter with prop
                assert leftprop
                success = addLabel(refname)
                assert success
                #assert filter.parent.parent, (refname, filter.parent)
                #filter.addLabel(refname, OBJECT)
                continue

            if joinFromLabel[refname] not in followingJoins:
                #only want to join with a more inner join,
                #so don't do this join yet
                unhandledJoins.append( joinInfo )
                continue

            refjoin = joinFromLabel.get(refname)
            topjoin = getTopJoin(refjoin, join)
            if topjoin is join:
                #XXX implement this: the easiest way would be to move the filter 
                #to the joincondition currently in place and do evalCrossJoin
                raise QueryException('multiple join conditions between the'
                ' same filter sets is not yet supported', filter)
            self.prepareJoinMove(topjoin)
            if topjoin.args:
                #?outer.prop = ?inner
                #add the join, setting leftposition to be the prop and (right)position to refjoin.name
                if rightprop is None:
                    rightprop = refjoin.name
                #?outer = ?inner.prop
                #add the join, setting (right)position to be the prop
                if leftprop is None:
                    leftprop = SUBJECT #set to id row
                #?outer.outerprop = ?inner.innerprop
                #both left and right props are set
                #add the join, setting leftposition to outerprop and (right)position to innerprop

                if maybe:
                    assert filter.parent.join in ['i', 'l']
                    jointype = 'l'
                else:
                    jointype = filter.parent.join
                join.appendArg( JoinConditionOp(topjoin, rightprop, jointype, leftprop) )
            else:
                #empty join occur in nested constructs like {* where id = ?tag}
                if filter.parent.parent:
                    filter.addLabel(topjoin.name, OBJECT)
                elif leftprop:
                    success = addLabel(topjoin.name)
                    assert success, (topjoin.name, refname, leftprop, rightprop, join)
                topjoin.parent = None
            
        return unhandledJoins

    def _makeHalfSimpleJoin(self, pred, joinname, leftname, leftops, rightname):
        filter = pred.parent
        #XXX currently labels to map to columns correctly with complexPredicates
        #note: this effectively disables this feature since this will be set to true
        if len(leftops) > 1 or filter.complexPredicates:
            return ()
        leftop = leftops[0]
        #XXX this matches ?other = func(?this.foo) but not ?this = func(?other.foo)
        #for that we need to move filter
        if leftop.parent is pred and leftname != joinname and rightname == joinname:            
            ignore, leftprop = getNameIfSimpleJoinRef(leftop, leftname, filter)
            assert ignore == leftname
            rightprop = '#'+self.nextAnonJoinId()
            assert not filter.labelFromPosition(LIST_POS+1)
            filter.addLabel(rightprop, LIST_POS+1)
            assert len(leftop.siblings) == 1
            #replace pred with rightside
            rightop = leftop.siblings[0]
            rightop.saveValue = rightprop
            filter.replaceArg(pred, rightop)
            return (leftname, leftprop, rightname, rightprop, filter, pred.maybe)
        return ()

def getTopJoin(join, top):
    candidate = parent = join
    while parent:
        if isinstance(parent, ResourceSetOp):
            if parent is top:
                return top #candidate
            candidate = parent
        parent = parent.parent
    return candidate

def getNameIfSimpleJoinRef(op, joinName, filter):
    if isinstance(op, Label):
        return op.name, None
    elif isinstance(op, Project):
        if op.name == OBJECT:
            propname = filter.labelFromPosition(OBJECT)
            assert propname
        else:
            propname = op.name
        return op.varref or joinName, propname
    return None, None


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
        filter.appendArg( Eq(Project(PROPERTY, maybe=p.maybe), PropString(name)) )
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
                propertyName = child.labelFromPosition(OBJECT)
                if not propertyName:
                    continue                
                if child.fromConstruct:
                    constructFilters.setdefault(propertyName, []).append(jc)
                else:
                    filterProjects.append(propertyName)
            
            #remove filters generated by construct expressions that match
            #where-clause filters
            for propertyName in filterProjects:
                constructJCs = constructFilters.get(propertyName)
                while constructJCs:
                    jc = constructJCs.pop()
                    jc.parent = None                    

            #look at the left-over construct filters and remove any duplicates
            #per property name
            for constructJCs in constructFilters.values():
                while len(constructJCs) > 1:
                    jc = constructJCs.pop()
                    jc.parent = None

def _fixMaybeFilters(projectPreds):
    from itertools import groupby
    projectPreds.sort()
    for k, v in groupby(projectPreds, lambda v: (v[0],v[1]) ):        
        v = list(v)
        if all(not ismaybe for (joinname, projectname, ismaybe, pred) in v):
            continue #no MAYBEs for this project
        #these are filters that have a predicate equivalent to the project
        #if the predicate isn't the maybe one, remove it
        #if it is and the filter has another predicate that not a simple selector
        #or tries to match null, move the maybe predicate to its own filter
        #otherwise make sure jointype = 'l'
        for joinname, projectname, ismaybe, pred in v:
            filter = pred.parent
            if not filter or not filter.parent:
                continue
            separate = False
            assert len(pred.siblings) <= 1
            for sib in pred.siblings:
                #if not simpleSelector(other): separate = True
                #XXX refactor with SimpleEngine._findSimplePredicates()
                if not isinstance(sib, Eq):
                    separate = True
                    break
                elif isinstance(sib.left, Project):
                    other = sib.right
                elif isinstance(sib.right, Project):
                    other = sib.left
                else:
                    separate = True
                    break
                if not other.isIndependent():
                    separate = True
                elif other == Constant(None):
                    separate = True

            if separate:
                filter.complexPredicates = True
                filter.removeLabel(projectname, OBJECT)
                for project in sib.depthfirst():
                    if isinstance(project, Project) and project.name == OBJECT:
                        #convert OBJECT to name
                        project.fields = [projectname]

                #if mabye move pred to separate join condition
                #otherwise just remove it
                if not ismaybe:
                    pred.parent = None                    
                else:
                    filter.parent.parent.appendArg( JoinConditionOp(
                        Filter(pred, objectlabel=projectname), join='l') )
            elif ismaybe:            
                if filter.parent.join not in ['i', 'l']:
                    raise QueryException(
                    'property with "maybe" can not be used here',filter.parent)
                filter.parent.join = 'l'
