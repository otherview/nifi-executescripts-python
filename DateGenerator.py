import sys
import traceback
import datetime
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback

class GenerateDates():
    def __init__(self, dateStart, dateEnd, dayStep, shardStart, shardEnd):
        self.dateStart = datetime.datetime.strptime(dateStart, '%Y-%m-%d').date()
        self.dateEnd = datetime.datetime.strptime(dateEnd, '%Y-%m-%d').date()
        self.dayStep = datetime.timedelta(days=dayStep)
        self.shardStart = shardStart
        self.shardEnd = shardEnd + 1
        pass

    def process(self):
        try:
            flowFileList = []

            while self.dateStart <= self.dateEnd:
                currentDateStart = self.dateStart.strftime('%Y-%m-%d')
                self.dateStart += self.dayStep
                currentDateEnd = self.dateStart.strftime('%Y-%m-%d')

                flowFile = session.create()
                flowFile = session.putAttribute(flowFile, "dateStart", currentDateStart)
                flowFile = session.putAttribute(flowFile, "dateEnd", currentDateEnd)
                flowFileList.append(flowFile)

            for shard in range(self.shardStart, self.shardEnd)
                for flow in flowFileList:
                    flow = session.putAttribute(flow, "shardId", shard)
                    session.transfer(flow, REL_SUCCESS))
                
        except:
            traceback.print_exc(file=sys.stdout)
            raise

dateStart = dateStart.getValue()
dateEnd = dateEnd.getValue()
dayStep = dayStep.getValue()
shardStart = shardStart.getValue()
shardEnd = shardEnd.getValue()
GenerateDates(dateStart, dateEnd, int(dayStep), int(shardStart), int(shardEnd)).process()
