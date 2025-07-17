select 
    * 
from 
    {{ source('staging', 'appearances') }}