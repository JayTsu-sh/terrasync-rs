import { defineAsyncComponent } from 'vue'

/**
 * 异步加载 ECharts + vue-echarts，避免在每个视图中重复相同的动态导入代码。
 */
export const VChart = defineAsyncComponent(async () => {
  const { use } = await import('echarts/core')
  const { PieChart, BarChart } = await import('echarts/charts')
  const { TitleComponent, TooltipComponent, LegendComponent, GridComponent, GraphicComponent } =
    await import('echarts/components')
  const { CanvasRenderer } = await import('echarts/renderers')
  use([
    PieChart,
    BarChart,
    TitleComponent,
    TooltipComponent,
    LegendComponent,
    GridComponent,
    GraphicComponent,
    CanvasRenderer,
  ])
  return import('vue-echarts')
})
