from org.apache.nifi.components.state import Scope
import time, json


class Item:
    def __init__(self, flowfile):
        self.attributes = {}
        for key,value in flowfile.getAttributes().iteritems():
            self.attributes[key] = value
        self.id = self.attributes['DocumentId']

    def PushFlowfile(self):
        flowfile = session.create()
        flowfile = session.putAllAttributes(flowfile, self.attributes)
        session.transfer(flowfile, REL_SUCCESS)


class StatefullMap:
    def __init__(self):
        self.stateMap = context.getStateManager().getState(Scope.LOCAL)
        tmpMap = self.stateMap.toMap()
        self.map = {}
        
        if self.stateMap.getVersion() != -1:
            for key,value in tmpMap.iteritems():
                self.map[key] = value 
        if 'timer' not in self.map:
            self.map['timer'] = time.time()
        self.timer = self.map['timer']

    def Add(self, item):
        self.map[item.id] = str(time.time())

    def Dump(self):
        if time.time() - self.timer >= 5:
            for key, item in self.map.iteritems():
                flowfile = session.create()
                flowfile = session.putAttribute(flowfile, "DocumentId", key)
                session.transfer(flowfile, REL_SUCCESS)
            context.getStateManager().clear(Scope.LOCAL)
        else:
            if self.stateMap.getVersion() == -1:
                context.getStateManager().setState(self.map, Scope.LOCAL)
            else:
                context.getStateManager().replace(self.stateMap, self.map, Scope.LOCAL)

flowFile = session.get()
if flowFile != None:

    context.getStateManager().clear(Scope.LOCAL)
    # stateMap = StatefullMap()
    # item = Item(flowFile)
    # stateMap.Add(item)
    # stateMap.Dump()

    # session.transfer(flowFile, REL_FAILURE)