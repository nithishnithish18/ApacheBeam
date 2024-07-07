import apache_beam as beam

# DoFn function 
class ProcessWords(beam.DoFn):
  
  def process(self, element, cutoff_length, marker):
    
    name = element.split(',')[1]
    
    if name.startswith(marker):
      return [name] 
    
    elif len(name) <= cutoff_length:
      return [beam.pvalue.TaggedOutput('Short_Names', name)]
    
    else:
      return [beam.pvalue.TaggedOutput('Long_Names', name)]
    

      

p = beam.Pipeline()

      
results = (
            p
            | beam.io.ReadFromText('dept_data.txt')
    
            | beam.ParDo(ProcessWords(), cutoff_length=4, marker='A').with_outputs('Short_Names', 'Long_Names', main='Names_A')

          )

short_collection = results.Short_Names
long_collection = results.Long_Names
startA_collection = results.Names_A  

# write to file  
short_collection | 'Write 1'>> beam.io.WriteToText('short')

# write to file
long_collection | 'Write 2'>> beam.io.WriteToText('long')

# write to file
startA_collection | 'Write 3'>> beam.io.WriteToText('start_a')

p.run()
