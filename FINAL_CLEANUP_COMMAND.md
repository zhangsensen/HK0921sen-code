# ✅ 扁平化完成后的验证指引

## 当前状态

- `hk_factor_discovery/` 目录已移除，所有核心代码均放置在仓库根目录的顶层包中。
- 包装器与兼容性层清理完毕，导入路径统一为 `application`, `phase1`, `phase2`, `utils` 等顶层模块。
- 测试、文档与示例同步更新，反映新的项目结构与导入方式。

## 推荐的日常校验

```bash
# 1. 运行快速静态检查（可选）
python ANALYSIS_SCRIPT.py --summary

# 2. 执行冒烟 / 回归测试
pytest -q

# 3. 验证核心导入是否正常
python - <<'PY'
from data_loader import HistoricalDataLoader
from phase1 import SingleFactorExplorer
from phase2 import MultiFactorCombiner
from utils.cache import InMemoryCache
print('Imports OK')
PY
```

## 发布前检查清单

- [ ] 没有重新引入 `hk_factor_discovery.*` 的导入语句
- [ ] 新增模块直接放置在顶层目录并添加必要的 `__init__.py`
- [ ] 文档示例与 README 反映最新代码结构
- [ ] `pytest` 全量通过，或对跳过的测试有明确说明

## FAQ

- **仍需兼容旧导入吗？** 不需要。旧路径已废弃，如外部项目依赖，请提供独立的迁移指南。
- **如何新增聚合导出？** 在相应包的 `__init__.py` 中维护 `__all__`，避免再创建包装器模块。
- **还需要执行 `/sc:cleanup` 命令吗？** 不再需要，该命令对应的重复代码问题已解决。

按以上步骤即可确保仓库在扁平化后始终保持干净、可维护的状态。
