import apache_beam as beam
import typing

class Employee:
  def __init__(self, id, name):
    self.id = id
    self.name = name

class EmployeeCoder(beam.coders.Coder):

  def encode(self, employee) :
    return ('%s:%s' % (employee.id, employee.name)).encode('utf-8')

  def decode(self, bytes):
    id, name = bytes.decode('utf-8').split(':')
    return Employee(id, name)

  def is_deterministic(self) -> bool:
    return True

beam.coders.registry.register_coder(Employee, EmployeeCoder)

def split(input):
  id,name,dept_id,dept_name,doj = input.split(',')
  return Employee(id,name), int(dept_id)

p = beam.Pipeline()

result = (p
          | beam.io.ReadFromText('dept_data.txt')
          | beam.Map(split)
          | beam.CombinePerKey(sum).with_input_types(typing.Tuple[Employee, int])
          | beam.io.WriteToText('data/output_new_final')
)

p.run()


