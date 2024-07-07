import apache_beam as beam

side_list=list()
with open ('exclude_ids.txt','r') as my_file:
  for line in my_file:
    side_list.append(line.rstrip())


class FilterUsingLength(beam.DoFn):
  def process(self, element, side_list, lower_bound, upper_bound):
    id = element.split(',')[0]
    name = element.split(',')[1]
    # id=id.decode('utf-8','ignore').encode("utf-8")
    element_list= element.split(',')
    if (lower_bound <= len(name) <= upper_bound) and id not in side_list:
      return [element_list]


p = beam.Pipeline()

# using pardo to filter names with length between 3 and 10
small_names =( 
                p
                | "Read from text file" >> beam.io.ReadFromText('dept_data.txt')
                | "ParDo with side inputs" >> beam.ParDo(FilterUsingLength(),side_list,1,5) 
                | beam.Filter(lambda record: record[3] == 'Accounts')
                | beam.Map(lambda record: (record[0]+ " " + record[1], 1))
                | beam.CombinePerKey(sum)
                | 'Write results' >> beam.io.WriteToText('data/output_new_final')
             )

p.run()


