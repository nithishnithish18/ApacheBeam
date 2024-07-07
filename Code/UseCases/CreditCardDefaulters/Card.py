import apache_beam as beam

class SplitRow(beam.DoFn):
  def process(self,element):
    return [element.split(',')]

class Mapkey(beam.DoFn):
  def process(self,element):
    return [(element[0], element[1:])]

class BusinessLogic(beam.DoFn):
  def process(self, element):
    (card_holder, details_list) = element
    spend_limit = int(details_list[4])
    total_spent = int(details_list[5])
    cleared_amount = int(details_list[7])
    counter = 0
    if total_spent > 0:
      short_payment_perc = (cleared_amount/total_spent)*100
      spent_perc = (total_spent/spend_limit)*100

      if spent_perc >= 100.0 and cleared_amount != total_spent:
        counter += 1
      elif short_payment_perc < 70.0:
        counter += 1
      elif spent_perc >= 100.0 and cleared_amount != total_spent and short_payment_perc < 70.0:
        counter += 1

      return [(card_holder, counter)]

    else:
      return [(card_holder,counter)]


p1 = beam.Pipeline()

card = (
    p1
    |beam.io.ReadFromText('cards.txt',skip_header_lines=1)
    |beam.ParDo(SplitRow())
    |beam.ParDo(Mapkey())
    |beam.ParDo(BusinessLogic())
    |beam.CombinePerKey(sum)
    |beam.io.WriteToText('data/output_new_final')
)
p1.run()