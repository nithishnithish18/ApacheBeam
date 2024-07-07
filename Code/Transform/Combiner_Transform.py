import apache_beam as beam

class AverageFn(beam.CombineFn):
  
  def create_accumulator(self):
    return (0.0,0)

  def add_input(self,accumulator,value):
    (total,count) = accumulator
    return (total+value,count+1)
  
  def merge_accumulators(self,accumulators):
    ind_sum,ind_count = zip(*accumulators)
    return (sum(ind_sum),sum(ind_count))

  def extract_output(self,accumulator):
    (total,count) = accumulator
    return total/count
  


p1 = beam.Pipeline()

small_sum = (
    
             p1
             |beam.Create([15,5,7,7,9,23,13,5])
             |beam.CombineGlobally(AverageFn())
             |beam.io.WriteToText('combine/output_new_final')
)
p1.run()
