import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/endpoints',
    },
    {
      path: '/endpoints',
      name: 'EndpointList',
      component: () => import('../views/EndpointList.vue'),
    },
    {
      path: '/endpoints/:id',
      name: 'EndpointDetail',
      component: () => import('../views/EndpointDetail.vue'),
    },
    {
      path: '/tasks',
      name: 'TaskList',
      component: () => import('../views/TaskList.vue'),
    },
    {
      path: '/tasks/:id',
      name: 'TaskDetail',
      component: () => import('../views/TaskDetail.vue'),
    },
    {
      path: '/settings',
      name: 'SystemSettings',
      component: () => import('../views/SystemSettings.vue'),
    },
  ],
})

export default router
