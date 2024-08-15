select s.user_name, s.node_name, s.session_id, s.transaction_id, s.transaction_start, l.object_name ,s.current_statement, max(a.memory_inuse_kb)/1024 MemMB

from sessions s

left join locks l on l.transaction_id = s.transaction_id

join resource_acquisitions a on a.transaction_id = s.transaction_id

where current_statement > ''

group by s.user_name, s.node_name, s.session_id, s.transaction_id, s.transaction_start, l.object_name ,s.current_statement;


