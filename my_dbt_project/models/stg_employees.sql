select *
from {{ source('postgres_src', 'employees') }}