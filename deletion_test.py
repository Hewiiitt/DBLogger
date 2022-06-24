from db_logger import DBAnalyser


db = DBAnalyser('./results.db')

experiment = db.get_experiments()

print(experiment)

# db.delete_experiment('7df8ee61-49ef-4846-a527-64420d524a94')
#
# experiment = db.get_experiments()
#
# print(experiment)