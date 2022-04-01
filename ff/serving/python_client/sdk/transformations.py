transformations_to_register = []

class Transformations:
    def __init__(self, feature: bool, name: str, variant = str, inputs = list, 
                        entity = str, fn_type = str, executor = str, frequency = str, ft_type = str):
        self.feature = feature
        self.name = name
        self.variant = variant
        self.inputs = inputs
        self.entity = entity
        self.fn_type = fn_type
        self.frequency = frequency
        self.ft_type = ft_type

def transformation(*args, **kwargs):

    def add_transformation(definition_func):

        transformation = Transformations(kwargs['feature'], definition_func.__name__ ,
                                            kwargs['variant'], kwargs['inputs'], 
                                            kwargs['entity'], kwargs['fn_type'], kwargs['executor'],  kwargs['frequency'], kwargs['ft_type'])
        definition_func()
        transformations_to_register.append(transformation)
    return add_transformation


@transformation(
    feature = True,
    variant = "7d",
    inputs = ["Transactions"],
    entity = "user",
    fn_type = "dataframe",
    executor = "demo-spark",
    frequency = "ff.orchestrator.hours(1)",
    ft_type = "numerical"

)
def user_transaction_count():
    """ Number of transactions the user performed in the last 7 days.
    """
    count = 10000000000
    return count
    # cutoff = datetime.datetime.now().AddDays(-7)
    # recent = transactions.loc[transactions["timestamp"] >= cutoff]
    # return recent.groupby("user").count()




