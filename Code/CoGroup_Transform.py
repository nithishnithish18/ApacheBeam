import apache_beam as beam

def retTuple(element):
  thisTuple = element.split(',')
  return (thisTuple[0],thisTuple[1:])

p1 = beam.Pipeline()

dept_rows = (
    p1
    |beam.io.ReadFromText('dept_data.txt')
    |beam.Map(retTuple)
)

low_rows = (
    p1
    |"read location data" >> beam.io.ReadFromText('location.txt')
    |"create tupe for locaiton data" >>beam.Map(retTuple)
)

result = ({'dept_data':dept_rows,'location_data':low_rows}
          |beam.CoGroupByKey()
          |beam.io.WriteToText('data/cogroup/output_new_final')
          )

p1.run()