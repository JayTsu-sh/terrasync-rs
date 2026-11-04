import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import TaskInfoPanel from '../TaskInfoPanel.vue'
import type { MigrationTask } from '../../api/tasks'
import type { Endpoint } from '../../api/endpoints'
const task: MigrationTask = { id: 'scan', name: '扫描', task_type: 'scan', status: 'cancelled', source_endpoint_id: 'source', config: {scan_resume:true}, created_at: '2026-11-01T00:00:00Z' }
const source = { id: 'source', name: '源目录', endpoint_type: 'local', config: {type:'local',path:'/data'} } as Endpoint
function render(value = task, dest: Endpoint | null = null) {
 return mount(TaskInfoPanel, { props: { task: value, sourceEndpoint: source, destEndpoint: dest }, global: { stubs: { EndpointCard: { props: ['endpoint','placeholder'], template: '<div data-test="endpoint">{{ endpoint?.name || placeholder?.label }}</div>' } } } })
}
describe('task type and cancellation presentation', () => {
 it('shows only the scanned location without a destination placeholder', () => {
  const wrapper=render()
  expect(wrapper.findAll('[data-test="endpoint"]')).toHaveLength(1)
  expect(wrapper.text()).not.toContain('无需目标')
 })
 it('labels a cancelled scan 已取消 while retaining resume', () => {
  const wrapper=render()
  expect(wrapper.text()).toContain('已取消')
  expect(wrapper.text()).not.toContain('已暂停')
  expect(wrapper.text()).toContain('继续扫描')
 })
 it('retains both endpoints for a migration', () => {
  expect(render({...task, task_type:'sync'}, {...source,id:'dest',name:'目标目录'}).findAll('[data-test="endpoint"]')).toHaveLength(2)
 })
 it('restarts a scan that did not enable checkpoints', () => {
  expect(render({...task, config:{scan_resume:false}}).text()).toContain('重新扫描')
 })
})
