import { test, expect } from '@playwright/test'
import { setupApiMocks } from './fixtures/api-mock'
import { makeTask } from './fixtures/mock-data'

test('scan shows source only, cancelled status and persisted configuration after editing', async ({ page }) => {
 const task=makeTask({id:'scan-config',name:'配置验证',task_type:'scan',status:'cancelled',source_endpoint_id:'ep-nfs-1',dest_endpoint_id:undefined,config:{concurrency:8,depth:2,enable_acl:true,peak_qos_rate:3}})
 await setupApiMocks(page,{tasks:[task],executions:[]})
 await page.goto('/tasks/scan-config')
 await expect(page.locator('.n-tag',{hasText:'已取消'})).toBeVisible()
 await expect(page.getByText('无需目标',{exact:true})).toHaveCount(0)
 await page.getByRole('button',{name:'编辑',exact:true}).click()
 const modal=page.locator('.n-modal')
 const request=page.waitForRequest(r=>r.method()==='PUT'&&r.url().endsWith('/tasks/scan-config'))
 await modal.locator('.n-form-item').filter({hasText:'并发度'}).locator('input').fill('32')
 await modal.getByRole('button',{name:'保存',exact:true}).click()
 const body=(await request).postDataJSON()
 expect(body.config).toMatchObject({concurrency:32,depth:2,enable_acl:true,peak_qos_rate:3})
 await expect(page.locator('.n-tabs-tab--active')).toHaveText('概览')
 await expect(page.getByText('32',{exact:true})).toBeVisible()
 await page.reload()
 await page.locator('.n-tabs-tab').filter({hasText:'概览'}).click()
 await expect(page.getByText('32',{exact:true})).toBeVisible()
 await expect(page.getByText('2',{exact:true})).toBeVisible()
})
