with attempted_calls as ({{ @attempted_calls }})
,
successful_calls as (
select owner_id, count(messaging_calllog.id) as call_count 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where duration > 60
and type = 'OUTGOING_TYPE'
group by owner_id)
,
connected_calls as (
select owner_id, count(messaging_calllog.id) as call_count 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where duration > 0
and type = 'OUTGOING_TYPE'
group by owner_id)
,
incoming_calls_connected as (
select owner_id, count(messaging_calllog.id) as call_count 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where type = 'INCOMING_TYPE'
group by owner_id
),
incoming_calls_missed as (
select owner_id, count(messaging_calllog.id) as call_count 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where type not in ('INCOMING_TYPE','OUTGOING_TYPE')
group by owner_id
),
talk_time_data as (
select owner_id, TO_CHAR(sum(messaging_calllog.duration)/60.0 * INTERVAL '1 minute', 'HH24:MI:SS') as talk_time_in_hours
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
group by owner_id
),
first_call_data as (
select owner_id, (min(messaging_calllog.start_time)) as min_call_start_time 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where type = 'OUTGOING_TYPE'
group by owner_id
),
last_call_data as (
select owner_id, (max(messaging_calllog.start_time)) as max_call_start_time 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where type = 'OUTGOING_TYPE'
group by owner_id
),
average_duration_data as (
select owner_id, TO_CHAR(avg(messaging_calllog.duration)/60.0 * INTERVAL '1 minute', 'HH24:MI:SS') as avg_call_duration_in_hours
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
group by owner_id
),
unique_patients_called_data as (
select owner_id, count(distinct messaging_calllog.to_phone) as unique_patient_count 
from messaging_calllog
inner join users_doctorprofile on users_doctorprofile.user_id = messaging_calllog.owner_id
where type = 'OUTGOING_TYPE'
group by owner_id
)

select users_user.phone as phone, users_doctorprofile.name, 
attempted_calls.call_count as attempted_call_count,
connected_calls.call_count as connected_call_count,
successful_calls.call_count as successful_call_count,
incoming_calls_connected.call_count as incoming_call_count,
talk_time_data.talk_time_in_hours,
first_call_data.min_call_start_time as first_call_time,
last_call_data.max_call_start_time as last_call_time,
average_duration_data.avg_call_duration_in_hours,
unique_patients_called_data.unique_patient_count
from users_doctorprofile
inner join users_user on users_doctorprofile.user_id = users_user.id
inner join users_organisation on users_organisation.id = users_user.organisation_id
inner join users_speciality on users_speciality.id = users_doctorprofile.speciality_id
inner join users_appconfig on users_appconfig.id = users_user.app_config_id
left join attempted_calls on users_doctorprofile.user_id = attempted_calls.owner_id
left join connected_calls on users_doctorprofile.user_id = connected_calls.owner_id
left join successful_calls on users_doctorprofile.user_id = successful_calls.owner_id
left join incoming_calls_connected on users_doctorprofile.user_id = incoming_calls_connected.owner_id
left join first_call_data on users_doctorprofile.user_id = first_call_data.owner_id
left join last_call_data on users_doctorprofile.user_id = last_call_data.owner_id
left join talk_time_data on users_doctorprofile.user_id = talk_time_data.owner_id
left join unique_patients_called_data on users_doctorprofile.user_id = unique_patients_called_data.owner_id
left join average_duration_data on users_doctorprofile.user_id = average_duration_data.owner_id
where users_speciality.name in ('Health Coach') and users_organisation.name = 'Curelink' and users_doctorprofile.app_access_state = 2
and users_appconfig.name = 'Curebot'
order by attempted_call_count asc


