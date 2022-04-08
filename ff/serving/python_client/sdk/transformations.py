import inspect
import ast
transformations_to_register = []

class Transformations:
    def __init__(self, feature: bool, name: str, variant = str, inputs = list, 
                        entity = str, fn_type = str, executor = str, frequency = str, ft_type = str, definition = str):
        self.feature = feature
        self.name = name
        self.variant = variant
        self.inputs = inputs
        self.entity = entity
        self.fn_type = fn_type
        self.frequency = frequency
        self.ft_type = ft_type
        self.definition = definition

def transformation(**kwargs):

    def add_transformation(definition_func):
        func_st = inspect.getsource(definition_func)
        tree = ast.parse(func_st)
        def_str = ""

        for function in tree.body:
            if isinstance(function,ast.FunctionDef):
            # In case there are loops in the definition
                lastBody = function.body[-1]
                while isinstance (lastBody,(ast.For,ast.While,ast.If)):
                    lastBody = lastBody.Body[-1]
                lastLine = lastBody.lineno
                if isinstance(func_st,str):
                    func_st = func_st.split("\n")
                for i , line in enumerate(func_st,1):
                    if i in range(function.lineno,lastLine+1):
                        def_str += line
        
        transformation = Transformations(kwargs['feature'], definition_func.__name__ ,
                                             kwargs['variant'], kwargs['inputs'], 
                                             kwargs['entity'], kwargs['fn_type'], kwargs['executor'],  kwargs['frequency'], kwargs['ft_type'], def_str)
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

    cutoff = "datetime.datetime.now().AddDays(-7)"
    i = [1, 2, 3]
    for e in i:
        print("o")
    recent = transactions.loc[transactions["timestamp"] >= cutoff]
    return recent.groupby("user").count()




