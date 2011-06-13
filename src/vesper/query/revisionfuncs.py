#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
    JSONQL functions for revision stores
'''
from vesper.query import *
from vesper.query.engine import SimpleQueryEngine, getColumn, flatten
from vesper.data import base
from vesper.data.base.graph import CTX_NS

def _getTransactionFunc(context, op):
    '''
    Return the transaction id for given statement as specified by the given property reference
    '''
    if not hasattr(context.initialModel, 'revisionModel'):
        return None
    
    if not isinstance(op, jqlAST.Project) or not op.varref:
        raise QueryException("getTransaction argument must be a full property reference", op)
    subjectPos = context.currentTupleset.findColumnPos(op.varref)
    if not subjectPos:        
        #print 'raise', context.currentTupleset.columns, 'row', context.currentRow
        raise QueryException("'%s' subject projection not found" % op.varref)
    col = list(getColumn(subjectPos, context.currentRow))
    assert len(col) == 1
    subject, pos, row = col[0]

    assert isinstance(op.name, (str,unicode))
    predicate = op.name
    predictatePos = context.currentTupleset.findColumnPos(op.name)
    if not predictatePos:
        #print 'raise', context.currentTupleset.columns, 'row', context.currentRow
        raise QueryException("'%s' projection not found" % op.name)
    
    vals = []
    for value, pos, row in getColumn(predictatePos, context.currentRow):
        objectType = row[pos+1]    
        stmt = base.Statement(subject, predicate, value, objectType) #XXX , context.scope
        vals.append( ResourceUri(context.initialModel.getContextForStatement(stmt)) )
    return vals

def getTxnId(context, op):
    return flatten(_getTransactionFunc(context, op))

def _getTxnProp(context, op, propname):
    txnids = _getTransactionFunc(context, op)
    if not txnids:
        return None
    def getVal(txnid):
        stmts = list(context.initialModel.revisionModel.filter(
                                    {0: txnid, 1: CTX_NS+propname}))
        if stmts:
            return stmts[0][2]
        else:
            return None
    return flatten([getVal(txnid) for txnid in txnids])

def getTxnTime(context, op):
    return _getTxnProp(context, op, 'createdOn')

def getTxnAuthor(context, op):
    return _getTxnProp(context, op, 'createdBy')

def getTxnComment(context, op):
    return _getTxnProp(context, op, 'comment')

SimpleQueryEngine.queryFunctions.addFunc('getTransactionId', getTxnId, ResourceUri, lazy=True)
SimpleQueryEngine.queryFunctions.addFunc('getTransactionTime', getTxnTime, NumberType, lazy=True)
SimpleQueryEngine.queryFunctions.addFunc('getTransactionAuthor', getTxnAuthor, StringType, lazy=True)
SimpleQueryEngine.queryFunctions.addFunc('getTransactionComment', getTxnComment, StringType, lazy=True)
