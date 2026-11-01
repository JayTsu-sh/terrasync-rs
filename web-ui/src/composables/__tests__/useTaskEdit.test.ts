import { describe,it,expect,vi,afterEach } from 'vitest'
vi.mock('../../api/tasks',()=>({updateTask:vi.fn()}))
vi.mock('../../api/endpoints',()=>({listEndpoints:vi.fn()}))
vi.mock('../../api/filters',()=>({getFilterFields:vi.fn()}))
import { updateTask } from '../../api/tasks'
import { useTaskEdit } from '../useTaskEdit'
import type { MigrationTask } from '../../api/tasks'
afterEach(()=>vi.resetAllMocks())
describe('persist edited task configuration',()=>{
 it('preserves existing scan options while changing concurrency',async()=>{
  const task:MigrationTask={id:'task',name:'scan',task_type:'scan',status:'cancelled',source_endpoint_id:'source',created_at:'',config:{depth:4,enable_acl:true,peak_qos_rate:3,concurrency:8}}
  const edit=useTaskEdit();edit.populateForm(task);edit.form.value.concurrency=32
  await edit.handleSubmit(task.id)
  expect(updateTask).toHaveBeenCalledWith('task',expect.objectContaining({config:expect.objectContaining({concurrency:32,depth:4,enable_acl:true,peak_qos_rate:3})}))
 })
})

it('keeps untouched filter expressions byte-for-byte when editing concurrency',async()=>{
 const task:MigrationTask={id:'task',name:'scan',task_type:'scan',status:'cancelled',source_endpoint_id:'source',created_at:'',config:{match_expr:'type==file',exclude_expr:'name=="*.tmp"',concurrency:8}}
 const edit=useTaskEdit();edit.populateForm(task);edit.form.value.concurrency=32
 await edit.handleSubmit(task.id)
 expect(updateTask).toHaveBeenCalledWith('task',expect.objectContaining({config:expect.objectContaining({match_expr:'type==file',exclude_expr:'name=="*.tmp"',concurrency:32})}))
})
