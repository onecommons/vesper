'''
    Schema support for RxPath, including an implementation of RDF Schema.

    Copyright (c) 2004-5 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
import StringIO, copy
from rx import RxPathUtils, RxPathModel
from RxPathUtils import BNODE_BASE, BNODE_BASE_LEN,RDF_MS_BASE,RDF_SCHEMA_BASE
from RxPathUtils import OBJECT_TYPE_RESOURCE, OBJECT_TYPE_LITERAL,Statement

class BaseSchema(object):
    '''
    A "null" schema that does nothing. Illustrates the minimum
    interfaces that must be implemented.
    '''
    isInstanceOf = None
    
    def __init__(self, model = None, autocommit=False):
        pass
    
    def isCompatibleType(self, testType, wantType):
        '''
        Returns whether or not the given type is compatible with
        a second type (e.g. equivalent to or a subclass of the second type)
        The given type may end in a "*", indicating a wild card.
        '''
        if wantType[-1] == '*':
            return testType.startswith(wantType[:-1])
        else:
            return testType == wantType
                         
    def isCompatibleProperty(self, testProp, wantProp):
        '''
        Returns whether or not the given property is compatible with
        a second property (e.g. equivalent to or a subproperty of the second type)
        The given property may end in a "*", indicating a wild card.
        '''        
        if wantProp[-1] == '*':
            return testProp.startswith(wantProp[:-1])
        else:
            return testProp == wantProp

    def setEntailmentTriggers(self, addCallBack, removeCallBack):
        '''
        Callbacks when entailments happen.
        '''

class RDFSSchema(BaseSchema, RxPathModel.MultiModel):
    
    #for a given context, deduce all additional statements and place them in another context.
    #the exception is rdf:type statements entailed subclass of 

    SUBPROPOF = u'http://www.w3.org/2000/01/rdf-schema#subPropertyOf'
    SUBCLASSOF = u'http://www.w3.org/2000/01/rdf-schema#subClassOf'

    inTransaction = False
    
    findCompatibleStatements = True
    
    addEntailmentCallBack = None
    removeEntailmentCallBack = None

    #from http://www.w3.org/TR/2004/rdf-mt/ -- axiomatic statements for RDF and RDFS 
#skip rdf:type rdf:Property -- we'll do this automatically as encountered
#skip rdfs:domain or rdfs:range with rdfs:Resource

    axiomaticTriples = '''<http://www.w3.org/2000/01/rdf-schema#Resource> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Resource> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#List> .
<http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
<http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
<http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/2000/01/rdf-schema#Class> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#subject> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#object> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#List> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/1999/02/22-rdf-syntax-ns#List> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Class> .
<http://www.w3.org/2000/01/rdf-schema#domain> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Class> .
<http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Class> .
<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
<http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Class> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/1999/02/22-rdf-syntax-ns#List> .
<http://www.w3.org/2000/01/rdf-schema#comment> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Literal> .
<http://www.w3.org/2000/01/rdf-schema#label> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2000/01/rdf-schema#Literal> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#Container> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#Container> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#Container> .
<http://www.w3.org/2000/01/rdf-schema#ContainerMembershipProperty> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
<http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.w3.org/2000/01/rdf-schema#seeAlso> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Datatype> .
<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#Literal> .
<http://www.w3.org/2000/01/rdf-schema#Datatype> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.w3.org/2000/01/rdf-schema#Class> .
'''

    rdfsSchema = [Statement(unicode(stmt[0]), unicode(stmt[1]), unicode(stmt[2]),
       objectType=unicode(stmt[3])) for stmt in RxPathUtils._parseTriples(
                                               StringIO.StringIO(axiomaticTriples))]

    autocommit = property(lambda self: self.model.autocommit,
                 lambda self, set: 
                   setattr(self.model, 'autocommit', set) or
                   setattr(self.entailments, 'autocommit', set)
                 )

    def __init__(self, model, entailmentModel=None):
        self.model = model
        self.entailments = entailmentModel or RxPathModel.TransactionMemModel()
        self.models = (model, self.entailments)        
        self.domains = {}
        self.ranges = {}
        self.inferences = {}  #map (resource, type) => refcount 
        
        #dictionary of type : ancestors (including self)
        self.supertypes = {}
        self.superproperties = {}

        #dictionary of type : descendants (including self)
        self.subtypes = { RDF_SCHEMA_BASE+u'Class': [RDF_SCHEMA_BASE+u'Class'] }
        self.supertypes = copy.deepcopy(self.subtypes)
        self.subproperties = {
                self.SUBCLASSOF :   [self.SUBCLASSOF],
                self.SUBPROPOF :  [self.SUBPROPOF],
                RDF_MS_BASE+u'type' : [RDF_MS_BASE+u'type']
        } 
        self.superproperties = copy.deepcopy(self.subproperties)

        self.subClassPreds = [self.SUBCLASSOF]
        self.subPropPreds =  [self.SUBPROPOF]
        self.typePreds =     [RDF_MS_BASE+u'type']
        
        self.currentSubProperties = self.subproperties
        self.currentSubTypes = self.subtypes
        self.currentSuperProperties = self.superproperties
        self.currentSuperTypes = self.supertypes

        autocommit = model.autocommit
        self.autocommit = True #disable _beginTxn() during init
        self.addToSchema(self.rdfsSchema)    
        #XXX completely unscalable!:
        self.addToSchema( model.getStatements() )
        self.autocommit = autocommit

    def isCompatibleType(self, testType, wantType):
        '''
        Is the given testType resource compatible with (equivalent to or a subtype of) the specified wantType?
        wantType can end in a * (to support the namespace:* node test in RxPath)
        '''
        if wantType == RDF_SCHEMA_BASE+'Resource': 
            return True
        return self._testCompatibility(self.currentSubTypes, testType, wantType)
    
    def isCompatibleProperty(self, testProp, wantProp):
        '''
        Is the given propery compatible with (equivalent to or a subpropery of) the specified property?
        wantProp can end in a * (to support the namespace:* node test in RxPath)
        '''
        return self._testCompatibility(self.currentSubProperties, testProp, wantProp)
    
    def _testCompatibility(self, map, testType, wantType):        
        #do the exact match test first in case we're calling this before we've completed setting up the schema            
        if testType == wantType:
            return True

        if wantType[-1] == '*':
            if testType.startswith(wantType[:-1]):
                return True
            for candidate in map:
                if candidate.startswith(wantType[:-1]):
                    subTypes = map[candidate]
                    if testType in subTypes:
                        return True
            return False
        else:            
            subTypes = map.get(wantType, [wantType])            
            return testType in subTypes
            
    def _makeClosure(self, aMap):
        #for each sub class, get its subclasses and append them
        def close(done, super, subs):
            done[super] = set(subs) 
            for sub in subs:                
                if not sub in done:                    
                    close(done, sub, aMap[sub])                
                done[super].update(done[sub])

        closure = {}           
        for key, value in aMap.items():
            close(closure, key, value)
        return dict([(x, list(y)) for x, y in closure.items()])

    def _addTypeStatement(self, stmt, addStmt=True):    
        if addStmt:
            self._addEntailment(stmt)    
        self.currentSubTypes.setdefault(stmt.object, [stmt.object])
        self.currentSuperTypes.setdefault(stmt.object, [stmt.object])
        key = (stmt.object, RDF_SCHEMA_BASE+u'Class')
        if key not in self.inferences:
            typeStmt = Statement(stmt.object, RDF_MS_BASE+u'type', RDF_SCHEMA_BASE+u'Class', OBJECT_TYPE_RESOURCE)
            self._addEntailment(typeStmt)
            self.inferences[key] = 1
        else:
            self.inferences[key] += 1
        
        if self.isCompatibleType(stmt.object, RDF_SCHEMA_BASE+u'Class'):
            self.currentSubTypes.setdefault(stmt.subject, [stmt.subject])
            self.currentSuperTypes.setdefault(stmt.subject, [stmt.subject])
        elif self.isCompatibleType(stmt.object, RDF_MS_BASE+u'Property'):
            self.currentSubProperties.setdefault(stmt.subject, [stmt.subject])
            self.currentSuperProperties.setdefault(stmt.subject, [stmt.subject])

    def isInstanceOf(self, resourceUri, typeUri):
        for superTypeUri in self.currentSuperTypes.get(typeUri, () ):
            if (resourceUri, superTypeUri) in self.inferences:
                return True
        return False
 
    def setEntailmentTriggers(self, addCallBack, removeCallBack):
        '''
        Callbacks when entailments happen.
        '''
        self.addEntailmentCallBack = addCallBack
        self.removeEntailmentCallBack = removeCallBack

    def _addEntailment(self,stmt):
        self.entailments.addStatement(stmt)
        if self.addEntailmentCallBack:
            self.addEntailmentCallBack(stmt)

    def _removeEntailment(self,stmt):
        self.entailments.removeStatement(stmt)
        if self.removeEntailmentCallBack:
            self.removeEntailmentCallBack(stmt)
    
    debug = 0
    def addToSchema(self, stmts):
        self._beginTxn()

        propsChanged = False
        typesChanged = False

        #you can declare subproperties to rdf:type, rdfs:subClassOf, rdfs:subPropertyOf
        #but it will only take effect in the next call to addToSchema
        #also they can not be removed consistently
        #thus they should be declared in the initial schemas
        for stmt in stmts:            
            #handle "rdf1" entailment rule in the RDF Semantics spec
            self._addEntailment(Statement(stmt.predicate, RDF_MS_BASE+u'type', 
                                RDF_MS_BASE+u'Property',OBJECT_TYPE_RESOURCE))
    
            #"rdfs4b" entailment rule (rdfs4a isn't necessary to make explicit, we just make this 
            #one explicit to guarantee that the object is the subject of at least one statement)
            if stmt.objectType == OBJECT_TYPE_RESOURCE:
                self.entailments.addStatement( Statement(stmt.object, 
                RDF_MS_BASE+u'type', 
                RDF_SCHEMA_BASE+u'Resource',OBJECT_TYPE_RESOURCE))
            
            #"rdfs2" and "rdfs3" entailment rules: infer types from domain and range of predicate  
            for predicate in self.currentSuperProperties.get(stmt.predicate,[stmt.predicate]):
                domains = self.domains.get(predicate, [])
                for domain in domains:
                    key = (stmt.subject, domain)
                    if key not in self.inferences:
                        typeStmt = Statement(stmt.subject, RDF_MS_BASE+u'type', 
                        domain, OBJECT_TYPE_RESOURCE)
                        self._addTypeStatement(typeStmt)
                        self.inferences[key] = 1
                    else:
                        self.inferences[key] += 1

                if stmt.objectType != OBJECT_TYPE_RESOURCE:
                    continue
                ranges = self.ranges.get(predicate, [])
                for range in ranges:
                    key = (stmt.object, range)
                    if key not in self.inferences:
                        typeStmt = Statement(stmt.object, RDF_MS_BASE+u'type', range, OBJECT_TYPE_RESOURCE)
                        self._addTypeStatement(typeStmt)
                        self.inferences[key] = 1
                    else:
                        self.inferences[key] += 1
                    #todo: we could do a consistency check here to make sure the object type conforms
                
            #the subclass and subproperty rules ("rdfs5" - "rdfs11") are handled dynamically
            #except for "rdfs8": uuu rdf:type rdfs:Class -> uuu rdfs:subClassOf rdfs:Resource
            #which isn't needed
            if stmt.predicate in self.subPropPreds:                
                self.currentSubProperties.setdefault(stmt.object, [stmt.object]).append(stmt.subject)
                #add this subproperty if this is the only reference to it so far
                self.currentSubProperties.setdefault(stmt.subject, [stmt.subject])

                self.currentSuperProperties.setdefault(stmt.subject, [stmt.subject]).append(stmt.object)
                #add this superproperty if this is the only reference to it so far
                self.currentSuperProperties.setdefault(stmt.object, [stmt.object])
                
                #if any superproperties are subject of any domain or range statements
                #deduce new type statements from existing statements 
                for predicate in self.currentSuperProperties[stmt.object]:
                    domaintypes = self.domains.get(predicate,[])
                    rangetypes = self.ranges.get(predicate,[]) 
                    if rangetypes or domaintypes:
                        for targetStmt in self.model.getStatements(predicate=predicate):
                            for type in domaintypes:
                                key = (targetStmt.subject, type)
                                refCount = self.inferences.get(key, 0)
                                if not refCount:
                                    typeStmt = Statement(targetStmt.subject,
                                            RDF_MS_BASE+u'type', type,
                                                         OBJECT_TYPE_RESOURCE)
                                    self._addTypeStatement(typeStmt)
                                    self.inferences[key ] = 1
                                else:
                                    self.inferences[key ] += 1

                            if stmt.objectType != OBJECT_TYPE_RESOURCE:
                                continue                                    
                            for type in rangetypes:
                                key = (targetStmt.object, type)
                                refCount = self.inferences.get(key, 0)
                                if not refCount:
                                    typeStmt = Statement(targetStmt.object,
                                            RDF_MS_BASE+u'type', type,
                                                    OBJECT_TYPE_RESOURCE)
                                    self._addTypeStatement(typeStmt)
                                    self.inferences[key ] = 1
                                else:
                                    self.inferences[key ] += 1
                            
                
                propsChanged = True
            elif stmt.predicate in self.subClassPreds:                
                self.currentSubTypes.setdefault(stmt.object, [stmt.object]).append(stmt.subject)
                #add this subclass if this is the only reference to it so far
                self.currentSubTypes.setdefault(stmt.subject, [stmt.subject])  

                self.currentSuperTypes.setdefault(stmt.subject, [stmt.subject]).append(stmt.object)
                #add this superclass if this is the only reference to it so far
                self.currentSuperTypes.setdefault(stmt.object, [stmt.object])
                
                typesChanged = True
            elif stmt.predicate in self.typePreds:
                self._addTypeStatement(stmt, addStmt=False)                                       
            else:
                self.currentSubProperties.setdefault(stmt.predicate, [stmt.predicate])
                self.currentSuperProperties.setdefault(stmt.predicate, [stmt.predicate])

                #if we're adding a domain or range statement infer type for resources that already have statements
                #with that predicate or subproperty
                if self.isCompatibleProperty(stmt.predicate, RDF_SCHEMA_BASE+u'domain'):
                    self.domains.setdefault(stmt.subject, []).append(stmt.object)
                    
                    for predicate in self.currentSubProperties[stmt.subject]:
                        for targetStmt in self.model.getStatements(predicate=stmt.subject):
                            key = (targetStmt.subject, stmt.object)
                            if key not in self.inferences:
                                typeStmt = Statement(targetStmt.subject,
                                    RDF_MS_BASE+u'type', stmt.object,
                                    OBJECT_TYPE_RESOURCE)
                                self._addTypeStatement(typeStmt)
                                self.inferences[key ] = 1
                            else:                                
                                self.inferences[key ] += 1
                
                if self.isCompatibleProperty(stmt.predicate, RDF_SCHEMA_BASE+u'range'):
                    self.ranges.setdefault(stmt.subject, []).append(stmt.object)
                    
                    for predicate in self.currentSubProperties[stmt.subject]:
                        for targetStmt in self.model.getStatements(
                            predicate=stmt.subject, objecttype=OBJECT_TYPE_RESOURCE):
                            key = (targetStmt.object, stmt.object)
                            if key not in self.inferences:
                                typeStmt = Statement(targetStmt.object,
                                    RDF_MS_BASE+u'type', stmt.object,
                                    OBJECT_TYPE_RESOURCE)
                                self._addTypeStatement(typeStmt)
                                self.inferences[key] = 1
                            else:
                                self.inferences[key] += 1                                                    

        if typesChanged:
            self.currentSubTypes = self._makeClosure(self.currentSubTypes)
            #XXX if self.saveSubtypes: 
            #    self.saveSubtypes()   XXX                
            if self.autocommit:
                self.subtypes = self.currentSubTypes
            
        if propsChanged:
            self.currentSubProperties = self._makeClosure(self.currentSubProperties)
            if self.autocommit:
                self.subproperties = self.currentSubProperties
        
            #just in case a subproperty of any of these were added
            self.subClassPreds = self.currentSubProperties[self.SUBCLASSOF]
            self.subPropPreds  = self.currentSubProperties[self.SUBPROPOF]
            self.typePreds     = self.currentSubProperties[RDF_MS_BASE+u'type']        
             
    def removeFromSchema(self, stmts):
        #todo: we don't remove resources from the properties or type dictionaries
        #(because its not clear when we can safely do that)
        #this means a formerly class or property resource can not be safely reused
        #as another type of resource without reloading the model
        self._beginTxn()
        
        propsChanged = False
        typesChanged = False

        for stmt in stmts:
            #todo: if a resource no longer appears in the model, we should remove the entailments
    
            #if a range or domain statement is being removed, remove any inferences based on it
            removeTypeInference = False
            if self.isCompatibleProperty(stmt.predicate, RDF_SCHEMA_BASE+u'domain'):
                self.domains[stmt.subject].remove(stmt.object)
                removeTypeInference = True
            if self.isCompatibleProperty(stmt.predicate, RDF_SCHEMA_BASE+u'range'):
                self.ranges[stmt.subject].remove(stmt.object)
                removeTypeInference = True 
    
            if removeTypeInference:
                for stmt in self.entailments.getStatements(
                    predicate=RDF_MS_BASE+u'type', object=stmt.object):
                    refCount = self.inferences[(stmt.subject, stmt.object)] - 1
                    #its possible that multiple domain and range rules
                    #entail the same type for a given resource
                    if refCount == 0:             
                        self._removeEntailment(stmt)
                        del self.inferences[(stmt.subject, stmt.object)]
                    else:
                        self.inferences[(stmt.subject, stmt.object)] = refCount
            
            for predicate in self.currentSuperProperties.get(stmt.predicate,
                                                             [stmt.predicate]):
                domains = self.domains.get(predicate, [])
                for domain in domains:
                    key = (stmt.subject, domain)
                    try:
                        refCount = self.inferences[key] - 1
                        if refCount == 0:             
                            self._removeEntailment(Statement(stmt.subject,
                                RDF_MS_BASE+u'type', domain, 
                                OBJECT_TYPE_RESOURCE))
                            del self.inferences[key]
                        else:
                            self.inferences[key] = refCount
                    except KeyError:
                        pass
    
                ranges = self.ranges.get(predicate, [])
                for range in ranges:                    
                    key = (stmt.object, range)
                    try:
                        refCount = self.inferences[key] - 1
                        if refCount == 0:
                            self._removeEntailment(Statement(stmt.object,
                                 RDF_MS_BASE+u'type', range, 
                                 OBJECT_TYPE_RESOURCE))
                            del self.inferences[key]
                        else:
                            self.inferences[key] = refCount
                    except KeyError:
                        pass

            if stmt.predicate in self.subPropPreds:
                try:
                    self.currentSubProperties[stmt.object].remove(stmt.subject)
                    self.currentSuperProperties[stmt.subject].remove(stmt.object)
                except KeyError, ValueError:
                    pass#todo warn if not found   
                 
                #find any domain and range statements for the super properties
                #and remove any type inferences made because a resource had this subproperty
                typeInferences = []
                for prop in self.currentSuperProperties[stmt.object]:
                    typeInferences.extend(self.domains.get(prop,[]) )
                    typeInferences.extend(self.ranges.get(prop,[]) )
                
                if typeInferences:
                    for prop in self.currentSubProperties[stmt.subject]:
                        #if any statement with the subproperty or sub-subproperty
                        for stmt in self.model.getStatements(predicate=prop):
                            for type in typeInferences:
                                key = (stmt.subject, type)
                                refCount = self.inferences[(stmt.subject, stmt.object)] - 1
                                #its possible that multiple domain and range rules entail the
                                #same type for a given resource
                                if refCount == 0:             
                                    self._removeEntailment(stmt)
                                    del self.inferences[(stmt.subject, stmt.object)]
                                else:
                                    self.inferences[(stmt.subject, stmt.object)] = refCount                            
                                            
                propsChanged = True

            if stmt.predicate in self.subClassPreds:
                try: 
                    self.currentSubTypes[stmt.object].remove(stmt.subject)
                    self.currentSuperTypes[stmt.subject].remove(stmt.object)
                except KeyError, ValueError:
                    pass#todo warn if not found                
                typesChanged = True            

        if typesChanged:
            newsubtypes = {}
            for k, v in self.currentSuperTypes.items():
                for supertype in v:
                    newsubtypes.setdefault(supertype, []).append(k)

            self.currentSubTypes = self._makeClosure(newsubtypes)
            if self.autocommit:
                self.subtypes = self.currentSubTypes
            
        if propsChanged:
            newsubprops = {}
            for k, v in self.currentSuperProperties.items():
                for superprop in v:
                    newsubprops.setdefault(superprop, []).append(k)
            
            self.currentSubProperties = self._makeClosure(newsubprops)
            if self.autocommit:
                self.subproperties = self.currentSubProperties

            #just in case a subproperty of any of these were removed
            self.subClassPreds = self.currentSubProperties[self.SUBCLASSOF]
            self.subPropPreds  = self.currentSubProperties[self.SUBPROPOF]
            self.typePreds     = self.currentSubProperties[RDF_MS_BASE+u'type']        
        
    def canRemoveDomain(self, stmt):
        #XXX finish and use 
        '''
        see if the stmt being remove triggered an domain entailment and find any
        the other domain rules that
        entail this type and see if the subject of the statement is subject
        of any of those properties, if not ok to remove type statement from subject
        '''
        types = self.domains.get(stmt.predicate,[])
        for type in types:
            predicates = self.domainsTypes.get(type, [])
            for prop in predicates:
                if prop == stmt.predicate:
                    continue
                if self.model.getStatements(stmt[0], prop):
                    return False
            self.entailments.removeStatement( (stmt[0], RDF+'type', type, R, SCHEMACTX) )

    def saveSubtypes(self):
        #XXX make this work and implement save_subtype_entailments = True
        adds, removes = diff(self.supertypes, self.currentSubTypes)
        
        for supertype, subtypes in adds.items():
            resources = set(stmt[0] for stmt in 
                        self.model.getStatements(predicate=supertype))
            for res in resources:
                
                for subtype in subtypes:
                    if subtype == supertype:
                        continue
                    typeStmt = Statement(res,
                            RDF_MS_BASE+u'type', subtype,
                            OBJECT_TYPE_RESOURCE)
                    self._addEntailment(typeStmt)                        

        for supertype, subtypes in removals.items():
            resources = set(stmt[0] for stmt in 
                        self.entailments.getStatements(predicate=supertype))

            for res in resources:                
                for subtype in subtypes:
                    if subtype == supertype:
                        continue
                    typeStmt = Statement(res,
                            RDF_MS_BASE+u'type', subtype,
                            OBJECT_TYPE_RESOURCE)
                    self._removeEntailment(typeStmt)                        
    
    def _beginTxn(self): 
        if not self.autocommit and not self.inTransaction:
            self.currentSubProperties = copy.deepcopy(self.subproperties)
            self.currentSubTypes = copy.deepcopy(self.subtypes)
            self.currentSuperProperties = copy.deepcopy(self.superproperties)
            self.currentSuperTypes = copy.deepcopy(self.supertypes)

            self.inTransaction = True

    def commit(self, **kw):
        if self.autocommit:            
            return
    
        super(RDFSSchema, self).commit(**kw)
        self.entailments.commit(**kw)
        
        self.subproperties = self.currentSubProperties        
        self.subtypes = self.currentSubTypes
        self.superproperties = self.currentSuperProperties
        self.supertypes = self.currentSuperTypes 
        
        self.inTransaction = False
    
    def rollback(self):
        super(RDFSSchema, self).rollback()
        #if self.entailments is not self.models[0]:
        self.entailments.rollback()
        self.currentSubProperties = self.subproperties
        self.currentSubTypes = self.subtypes
        self.currentSuperProperties = self.superproperties
        self.currentSuperTypes = self.supertypes

        #just in case a subproperty of any of these changed
        self.subClassPreds = self.currentSubProperties[self.SUBCLASSOF]
        self.subPropPreds  = self.currentSubProperties[self.SUBPROPOF]
        self.typePreds     = self.currentSubProperties[RDF_MS_BASE+u'type']        

        self.inTransaction = False

    ### Model Operations ###                   
    def addStatement(self, stmt ):
        '''add the specified statement to the model'''
        self.addToSchema( (stmt,) )
        self.model.addStatement(stmt)
                      
    def removeStatement(self, stmt ):
        '''Removes the statement. If 'scope' isn't specified, the statement
           will be removed from all contexts it appears in.
        '''        
        self.removeFromSchema( (stmt,) )
        self.model.removeStatement(stmt)
        self.entailments.removeStatement(stmt)

    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        
        if not self.findCompatibleStatements:
            return super(RDFSSchema, self).getStatements(subject,
                        predicate,object,objecttype,context, asQuad, hints)

        
        if predicate in self.typePreds:
            submap = self.currentSubTypes
            test = object
            pos = 3
            ranges = set()
            domains = set()
        else:
            submap = self.currentSubProperties
            test = predicate
            pos = 2
            ranges = domains = ()
        
        statements = []     
        changed = 0
        #note: if `test` is None we will retrieve non-entailed statements
        #to get all supertypes or superproperties, you need to explicitly query
        for compatible in submap.get(test, [test]):
            if pos == 2:
                predicate = compatible
            elif pos == 3:
                object = compatible
                #handle subprops
                #ranges += self.rangeProps.get(object, [])
                #domains += self.domainsProps.get(object, [])
                
            #XXX handle hints intelligently, see super() implementation
            moreStatements = super(RDFSSchema, self).getStatements(subject,
                                predicate,object,objecttype,context, asQuad)            
            if moreStatements:
                changed += 1
                statements.extend(moreStatements)

        #XXX not yet implemented:
        #all props and subprops that have a domain of requested type or subtype
        if subject is not None:
            drhints={'exist':1}
        else:
            drhints=None
        for prop in domains:
            moreStatements = super(RDFSSchema, self).getStatements(subject,
                            prop,scope=context, hints=drhints)
            if moreStatements:
                changed += 1
                #reconstruct rdf:type statement
                statements.extend( set(Statement(s[0],predicate, object,
                    OBJECT_TYPE_RESOURCE, s[4]) for s in moreStatements) )

        #all props and subprops that have a range of requested type or subtype
        for prop in ranges:
            moreStatements = super(RDFSSchema, self).getStatements(predicate=prop,
                object=subject, objecttype=OBJECT_TYPE_RESOURCE,
                scope=context, hint=drhints)
            if moreStatements:
                changed += 1
                #reconstruct rdf:type statement
                statements.extend( set(Statement(s[2],predicate, object,
                    OBJECT_TYPE_RESOURCE, s[4]) for s in moreStatements)   )

        if changed > 1 or hints:        
            statements.sort()
            return RxPathModel.removeDupStatementsFromSortedList(statements, asQuad, **(hints or {}))
        else:
            return statements            

defaultSchemaClass = BaseSchema #RDFSSchema
