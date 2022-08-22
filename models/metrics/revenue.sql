select 
    * 
from {{ 
    metrics.calculate(
    metric('LIFETIME_VALUE'),
    grain='week',
    dimensions=['NUMBER_OF_ORDERS']
)
}}