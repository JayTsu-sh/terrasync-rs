import { afterEach, expect, it, vi } from 'vitest'
vi.mock('../../api/tasks', () => ({ createTask: vi.fn() }))
vi.mock('../../api/endpoints', () => ({ listEndpoints: vi.fn() }))
vi.mock('../../api/filters', () => ({ getFilterFields: vi.fn() }))
import { createTask } from '../../api/tasks'
import { useTaskCreate } from '../useTaskCreate'

afterEach(() => vi.resetAllMocks())

it('copy resume defaults off, is saved explicitly and resets for a new task', async () => {
  const create = useTaskCreate()
  create.form.value.name = 'copy'
  create.form.value.source_endpoint_id = 'source'
  create.form.value.dest_endpoint_id = 'target'
  await create.handleSubmit()
  expect(createTask).toHaveBeenLastCalledWith(expect.objectContaining({ config: expect.objectContaining({ copy_resume: false }) }))
  create.form.value.copy_resume = true
  create.form.value.scan_resume = true
  await create.handleSubmit()
  expect(createTask).toHaveBeenLastCalledWith(expect.objectContaining({ config: expect.objectContaining({ copy_resume: true, scan_resume: false }) }))
  create.resetForm()
  expect(create.form.value.copy_resume).toBe(false)
})

it('copy resume does not enable scan checkpoints after changing task type', async () => {
  const create = useTaskCreate()
  create.form.value.name = 'scan'
  create.form.value.task_type = 'scan'
  create.form.value.source_endpoint_id = 'source'
  create.form.value.copy_resume = true
  await create.handleSubmit()
  expect(createTask).toHaveBeenLastCalledWith(expect.objectContaining({ config: expect.objectContaining({ copy_resume: false, scan_resume: false }) }))
})
