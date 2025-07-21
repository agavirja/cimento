import pandas as pd
import json
from numbers import Number

from functions.general_functions import getPredialLink

def main(inputvar={}):

    # ——————————————————————————————————————————————————————————————————————— #
    # Variables inputvar: 
    # ——————————————————————————————————————————————————————————————————————— #
    year = int(inputvar['year']) if 'year' in inputvar and isinstance(inputvar['year'],Number) else None
    chip = inputvar['chip'] if 'chip' in inputvar and isinstance(inputvar['chip'],str) else None

    # ——————————————————————————————————————————————————————————————————————— #
    # Estructura de datos
    # ——————————————————————————————————————————————————————————————————————— #
    output = {'link':None}
    if isinstance(year,Number) and year>0 and  isinstance(chip,str) and chip!='':
        output = {'link':getPredialLink(year,chip)}

    return output