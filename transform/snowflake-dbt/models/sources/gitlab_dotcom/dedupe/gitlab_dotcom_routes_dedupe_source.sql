select *
from {{ source("gitlab_dotcom", "routes") }}
qualify row_number() over (partition by id order by _uploaded_at desc) = 1
