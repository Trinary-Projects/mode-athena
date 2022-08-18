select owner_id, count(messaging_calllog.id) as call_count 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where duration > 60
and type = 'OUTGOING_TYPE'
group by owner_id
