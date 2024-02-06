import azure.functions as func

from praca_blueprint import praca_blueprint as bp
from olx_blueprint import olx_blueprint as bp1
from pracuj_blueprint import pracuj_blueprint as bp2
from aplikuj_blueprint import aplikuj_blueprint as bp3
from znajdzprace_blueprint import znajdzprace_blueprint as bp4
from ofertypracagov_blueprint import ofertypracagov_blueprint as bp5

app = func.FunctionApp()
app.register_functions(bp)
app.register_functions(bp1)
app.register_functions(bp2)
app.register_functions(bp3)
app.register_functions(bp4)
app.register_functions(bp5)
