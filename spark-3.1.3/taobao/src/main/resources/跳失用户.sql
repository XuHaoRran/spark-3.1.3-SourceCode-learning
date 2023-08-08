select count(*)
from
    (
        select user_id from dwd_user_behavior
        group by user_id
        having count(behavior)=1
    ) a;
