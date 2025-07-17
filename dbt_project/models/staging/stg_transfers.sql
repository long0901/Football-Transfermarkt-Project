select
    *
from
    {{ source('staging', 'transfers') }}
