import apache_beam as beam

class SplitRow(beam.DoFn):
    def process(self,element):
        return [element.split(',')]

class Filter(beam.DoFn):
  def process(self,element):
    if element[3] == 'Accounts':
      return [element]

class PairEmp(beam.DoFn):
  def process(self,element):
    return [(element[1] +" "+ element[3],1)]

class Counting(beam.DoFn):
  def process(self,element):
    (key,value) = element
    return [(key,sum(value))]

p1 = beam.Pipeline()

attendance_count = (
    
p1
|beam.io.ReadFromText('dept_data.txt')
|beam.ParDo(SplitRow())
|beam.ParDo(Filter())
|beam.ParDo(PairEmp())
|beam.GroupByKey()
|beam.ParDo(Counting())
|beam.io.WriteToText('data/output_new_final')
)
p1.run()