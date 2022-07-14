select *
from {{ source("gitlab_dotcom", "routes") }}
qualify row_number() OVER (partition by id order by _uploaded_at desc) = 1
