const statusDot = document.getElementById("statusDot");
const statusText = document.getElementById("statusText");
const specSelect = document.getElementById("specSelect");
const specPath = document.getElementById("specPath");
const runBtn = document.getElementById("runBtn");
const traceBtn = document.getElementById("traceBtn");
const tracePath = document.getElementById("tracePath");
const outputPath = document.getElementById("outputPath");
const traceType = document.getElementById("traceType");
const mappingPath = document.getElementById("mappingPath");
const jobsList = document.getElementById("jobsList");
const jobCount = document.getElementById("jobCount");
const logBox = document.getElementById("logBox");
const logStatus = document.getElementById("logStatus");
const schemaPreview = document.getElementById("schemaPreview");
const schemaPreviewTag = document.getElementById("schemaPreviewTag");
const schemaGraph = document.getElementById("schemaGraph");
const schemaOverview = document.getElementById("schemaOverview");
const schemaFileSelect = document.getElementById("schemaFileSelect");
const refreshSchemaFiles = document.getElementById("refreshSchemaFiles");
const loadSchemaBtn = document.getElementById("loadSchemaBtn");
const schemaTableList = document.getElementById("schemaTableList");
const schemaSourceType = document.getElementById("schemaSourceType");
const schemaDataPath = document.getElementById("schemaDataPath");
const schemaDbConfigPath = document.getElementById("schemaDbConfigPath");
const schemaName = document.getElementById("schemaName");
const schemaSampleSize = document.getElementById("schemaSampleSize");
const schemaOutputPath = document.getElementById("schemaOutputPath");
const schemaBtn = document.getElementById("schemaBtn");
const visualizeSchemaBtn = document.getElementById("visualizeSchemaBtn");
const schemaLiveLog = document.getElementById("schemaLiveLog");
const schemaLiveStatus = document.getElementById("schemaLiveStatus");
const schemaPathRow = document.getElementById("schemaPathRow");
const schemaDbConfigRow = document.getElementById("schemaDbConfigRow");
const schemaNameRow = document.getElementById("schemaNameRow");
const schemaCsvExtras = document.getElementById("schemaCsvExtras");
const schemaDbExtras = document.getElementById("schemaDbExtras");
const schemaDataFile = document.getElementById("schemaDataFile");
const schemaDbConfigFile = document.getElementById("schemaDbConfigFile");
const schemaCsvText = document.getElementById("schemaCsvText");
const schemaDbConfigText = document.getElementById("schemaDbConfigText");
const saveCsvBtn = document.getElementById("saveCsvBtn");
const saveDbConfigBtn = document.getElementById("saveDbConfigBtn");
const schemaDataUploadStatus = document.getElementById("schemaDataUploadStatus");
const schemaDbUploadStatus = document.getElementById("schemaDbUploadStatus");
const schemaCsvSaveStatus = document.getElementById("schemaCsvSaveStatus");
const schemaDbSaveStatus = document.getElementById("schemaDbSaveStatus");
const schemaDbConfigSelect = document.getElementById("schemaDbConfigSelect");
const refreshDbConfigs = document.getElementById("refreshDbConfigs");
const dataDriftPane = document.getElementById("dataDriftPane");
const workloadDriftPane = document.getElementById("workloadDriftPane");
const driftDataEnabled = document.getElementById("driftDataEnabled");
const driftWorkloadEnabled = document.getElementById("driftWorkloadEnabled");
const addDataDriftBtn = document.getElementById("addDataDriftBtn");
const addWorkloadDriftBtn = document.getElementById("addWorkloadDriftBtn");
const dataDriftList = document.getElementById("dataDriftList");
const workloadDriftList = document.getElementById("workloadDriftList");
const buildSpecBtn = document.getElementById("buildSpecBtn");
const generatedSpecPath = document.getElementById("generatedSpecPath");
const buildSpecStatus = document.getElementById("buildSpecStatus");
const sideNavEl = document.querySelector(".side-nav");
const navToggleBtn = document.getElementById("navToggleBtn");
const languageSelect = document.getElementById("languageSelect");
const sideNavLinks = Array.from(document.querySelectorAll(".side-nav-link"));

const I18N = {
  zh: {
    page_title: "DriftBench 服务台",
    nav_expand: "展开导航",
    nav_collapse: "收起导航",
    nav_aria: "页面导航",
    nav_title: "导航",
    nav_schema_source: "Schema 数据源",
    nav_schema_run: "生成 Schema",
    nav_schema_preview: "Schema 预览",
    nav_drift_builder: "Drift 设计",
    hero_title: "本地服务台",
    hero_subtitle: "在浏览器中启动 DriftSpec 任务、查看日志与结果路径，无需手动命令行。",
    status_connecting: "连接服务中...",
    status_connected: "服务已连接",
    status_disconnected: "服务未连接",
    language_label: "语言",
    language_zh: "中文",
    language_en: "English",
    schema_source_title: "Schema 数据源",
    schema_source_chip: "数据源",
    source_type_label: "Source 类型",
    data_path_label: "数据路径",
    data_path_placeholder: "data/census_original.csv",
    import_csv_label: "导入 CSV 文件",
    paste_csv_label: "粘贴 CSV 内容 (可选)",
    csv_text_placeholder: "col1,col2\n1,2",
    save_csv_btn: "保存 CSV",
    saved_config_path_label: "已保存配置路径",
    saved_config_path_placeholder: "通过上传/粘贴生成",
    import_db_config_label: "导入 DB 配置文件",
    select_saved_config_label: "选择已有配置",
    loading_option: "加载中...",
    refresh_btn: "刷新",
    paste_db_json_label: "粘贴 DB 配置 JSON",
    save_config_btn: "保存配置",
    db_schema_name_label: "DB schema 名称",
    db_schema_name_placeholder: "public",
    generate_schema_title: "生成 Schema",
    run_chip: "执行",
    sample_size_label: "Sample Size (可选)",
    sample_size_placeholder: "1000",
    schema_output_path_label: "输出 schema 路径 (可选)",
    schema_output_path_placeholder: "driftbench_service/schemas/my_schema.json",
    generate_schema_btn: "生成 Schema",
    visualize_schema_btn: "可视化 Schema",
    schema_hint: "CSV 会抽样统计分布；Postgres 需要 psycopg2。",
    schema_live_log_title: "Schema 实时日志",
    wait_task: "等待任务",
    schema_live_log_idle: "点击“生成 Schema”后显示实时日志。",
    schema_preview_title: "Schema 预览",
    schema_preview_pending: "待分析",
    select_schema_file: "选择 schema 文件...",
    load_btn: "加载",
    schema_graph_empty: "加载 schema 后显示表关系图。",
    schema_overview_empty: "加载 schema 后显示整体特征。",
    schema_table_list_empty: "加载 schema 后，直接点击上方关系图中的表节点进行选择（支持多选）。",
    schema_preview_empty: "点击上方关系图中的表节点后，这里显示前几行与表结构。",
    drift_builder_title: "Drift 设计",
    builder_chip: "构建器",
    drift_builder_hint: "先在 Schema 预览中点击目标 table，再在这里配置 Data / Workload drift。",
    enable_data_drift: "启用 Data Drift",
    add_data_drift_btn: "添加 Data Drift",
    no_data_drift: "暂无 Data drift，点击“添加 Data Drift”。",
    enable_workload_drift: "启用 Workload Drift",
    add_workload_drift_btn: "添加 Workload Drift",
    no_workload_drift: "暂无 Workload drift，点击“添加 Workload Drift”。",
    driftspec_output_path_label: "输出 DriftSpec 路径",
    driftspec_output_path_placeholder: "driftspec/generated/service_spec.yaml",
    build_driftspec_btn: "生成 DriftSpec",
    build_spec_idle: "尚未生成 DriftSpec。",
    run_driftspec_title: "运行 DriftSpec",
    yaml_chip: "YAML",
    examples_label: "示例",
    yaml_path_label: "YAML 路径",
    yaml_path_placeholder: "driftspec/examples/demo_data_single.yaml",
    run_job_btn: "运行任务",
    repo_path_hint: "路径需在本仓库内，支持相对路径。",
    generate_chip: "生成",
    trace_spec_title: "Trace → Spec",
    trace_path_label: "Trace 路径",
    trace_path_placeholder: "driftspec/trace_inputs/trace_data_mock.csv",
    trace_output_yaml_label: "输出 YAML 路径",
    trace_output_yaml_placeholder: "driftspec/generated/trace_data_mock.yaml",
    trace_type_label: "Trace 类型",
    trace_type_auto: "自动推断",
    mapping_path_label: "Mapping 路径 (可选)",
    mapping_path_placeholder: "driftspec/trace_inputs/mapping.json",
    generate_driftspec_btn: "生成 DriftSpec",
    no_jobs: "暂无任务",
    jobs_title: "任务",
    logs_title: "日志",
    logs_idle: "尚未开始任务。",
    select_example: "选择示例...",
    load_failed: "加载失败",
    delete_btn: "删除",
    delete_confirm: "删除 job #{jobId} ?",
    job_deleted: "该任务已删除。",
    delete_failed: "删除失败: {error}",
    no_logs: "暂无日志。",
    log_trimmed: "[日志已裁剪: {count} 行]",
    executing: "正在执行...",
    schema_loading: "加载中",
    schema_updated: "已更新",
    failed_short: "失败",
    read_schema_failed: "读取 schema 失败",
    select_table_in_graph: "请在上方图中点击表节点进行选择。",
    no_tables_to_show: "没有可展示的表。",
    graph_title_no_edges: "未检测到关系边，仅展示表/键信息（可拖动表节点）",
    graph_title_fk: "关系来源: 外键约束（可拖动表节点）",
    graph_title_inferred: "关系来源: 列名推断（可拖动表节点）",
    graph_summary: "{title} · 边数: {count} · 点击节点可多选，拖动可调整位置",
    graph_aria: "schema relation graph",
    more_count: "+{count} 更多",
    load_schema_first: "请先加载 schema。",
    click_graph_select_multi: "请直接点击上方图中的表节点进行选择（支持多选）。",
    rows_short: "行",
    cols_short: "列",
    no_sample_rows: "暂无样例行。",
    no_column_info: "无字段信息。",
    click_graph_show_schema: "请在上方图中点击表节点后，这里会显示该表前几行与 schema。",
    wait_sample_rows: "等待加载样例行...",
    loading_sample_rows: "正在加载样例行...",
    sample_rows_load_failed: "样例行加载失败: {error}",
    unknown_error: "unknown error",
    expand: "展开",
    collapse: "折叠",
    collapsed_hint: "当前已折叠，点击“展开”查看前5行和表结构。",
    top_rows: "前5行数据（最多）",
    compact_schema_title: "表结构（紧凑）",
    schema_empty: "Schema 为空",
    no_selectable_table: "无可选 table",
    table_label: "表",
    column_label: "字段",
    total_rows_label: "总行数",
    fk_edges_label: "FK/边",
    unable_load_logs: "无法加载日志",
    enter_yaml_path: "请输入 YAML 路径",
    run_failed: "运行失败: {error}",
    enter_trace_and_output: "请输入 trace 路径和输出路径",
    generate_failed: "生成失败: {error}",
    temporal_drift_label: "Temporal Drift",
    temporal_generate_pattern: "为该 drift 生成时间模式",
    timestamp_column_label: "Timestamp 列名",
    data_item_title: "Data #{index}",
    workload_item_title: "Workload #{index}",
    remove_btn: "删除",
    name_optional: "名称 (可选)",
    drift_type_label: "Drift 类型",
    data_drift_temporal_hint: "当前 drift 已是时间型，不再追加 temporal drift。",
    columns_csv_label: "columns (逗号分隔)",
    baseline_hint: "baseline 使用默认模板参数，不附加额外漂移参数。",
    data_drift_csv_required: "Data Drift 目前要求 CSV 数据源，请先切换 Source 类型为 csv。",
    data_drift_csv_path_required: "Data Drift 需要 data_source.path（CSV 路径）。",
    add_one_data_drift: "请至少添加一个 Data Drift 条目。",
    outlier_column_required: "outlier_injection 需要 column。",
    add_one_workload_drift: "请至少添加一个 Workload Drift 条目。",
    workload_csv_path_required: "Workload Drift (csv) 需要数据路径。",
    workload_postgres_config_required: "Workload Drift (postgres) 需要 DB 配置路径，请先上传/保存配置。",
    unsupported_source_type: "暂不支持的数据源类型: {sourceType}",
    selectivity_column_required: "selectivity 类型需要列名。",
    distribution_column_required: "distribution 类型需要列名。",
    enable_one_drift_card: "请至少启用一个 Drift 卡片。",
    building_driftspec: "正在生成 DriftSpec...",
    generated_paths: "已生成: {paths}",
    upload_or_paste_db_config: "请上传或粘贴 DB 配置 JSON",
    saved_path: "已保存: {path}",
    save_db_config_failed: "保存 DB 配置失败: {error}",
    enter_data_path: "请输入数据路径",
    schema_job_submitted: "任务已提交，等待后端输出日志...",
    read_file_failed: "读取文件失败",
    select_config: "选择配置...",
    uploading: "上传中...",
    saving: "保存中...",
    failed_with_error: "失败: {error}",
    enter_csv_content: "请输入 CSV 内容",
    enter_db_json: "请输入 DB 配置 JSON",
    select_schema_first: "请先选择 schema 文件",
    no_visualizable_schema: "没有可视化的 schema 文件，请先生成 Schema",
    build_failed: "生成失败: {error}",
    job_label: "任务",
  },
  en: {
    page_title: "DriftBench Console",
    nav_expand: "Expand navigation",
    nav_collapse: "Collapse navigation",
    nav_aria: "Page navigation",
    nav_title: "Navigation",
    nav_schema_source: "Schema Source",
    nav_schema_run: "Generate Schema",
    nav_schema_preview: "Schema Preview",
    nav_drift_builder: "Drift Design",
    hero_title: "Local Console",
    hero_subtitle: "Run DriftSpec jobs, inspect logs, and review output paths from the browser instead of the CLI.",
    status_connecting: "Connecting to service...",
    status_connected: "Service connected",
    status_disconnected: "Service disconnected",
    language_label: "Language",
    language_zh: "Chinese",
    language_en: "English",
    schema_source_title: "Schema Source",
    schema_source_chip: "Source",
    source_type_label: "Source Type",
    data_path_label: "Data Path",
    data_path_placeholder: "data/census_original.csv",
    import_csv_label: "Upload CSV File",
    paste_csv_label: "Paste CSV Content (Optional)",
    csv_text_placeholder: "col1,col2\n1,2",
    save_csv_btn: "Save CSV",
    saved_config_path_label: "Saved Config Path",
    saved_config_path_placeholder: "Generated from upload or pasted text",
    import_db_config_label: "Upload DB Config File",
    select_saved_config_label: "Choose Existing Config",
    loading_option: "Loading...",
    refresh_btn: "Refresh",
    paste_db_json_label: "Paste DB Config JSON",
    save_config_btn: "Save Config",
    db_schema_name_label: "DB Schema Name",
    db_schema_name_placeholder: "public",
    generate_schema_title: "Generate Schema",
    run_chip: "Run",
    sample_size_label: "Sample Size (Optional)",
    sample_size_placeholder: "1000",
    schema_output_path_label: "Schema Output Path (Optional)",
    schema_output_path_placeholder: "driftbench_service/schemas/my_schema.json",
    generate_schema_btn: "Generate Schema",
    visualize_schema_btn: "Visualize Schema",
    schema_hint: "CSV uses sampled distribution stats; Postgres requires psycopg2.",
    schema_live_log_title: "Schema Live Log",
    wait_task: "Waiting",
    schema_live_log_idle: "Live schema logs will appear after you click \"Generate Schema\".",
    schema_preview_title: "Schema Preview",
    schema_preview_pending: "Pending",
    select_schema_file: "Select a schema file...",
    load_btn: "Load",
    schema_graph_empty: "The table relationship graph will appear after a schema is loaded.",
    schema_overview_empty: "The schema summary will appear after a schema is loaded.",
    schema_table_list_empty: "Load a schema, then click table nodes in the graph above to select them. Multi-select is supported.",
    schema_preview_empty: "After you click a table node in the graph above, sample rows and schema details will appear here.",
    drift_builder_title: "Drift Design",
    builder_chip: "Builder",
    drift_builder_hint: "Pick target tables in Schema Preview first, then configure Data / Workload drift here.",
    enable_data_drift: "Enable Data Drift",
    add_data_drift_btn: "Add Data Drift",
    no_data_drift: "No Data drift yet. Click \"Add Data Drift\".",
    enable_workload_drift: "Enable Workload Drift",
    add_workload_drift_btn: "Add Workload Drift",
    no_workload_drift: "No Workload drift yet. Click \"Add Workload Drift\".",
    driftspec_output_path_label: "DriftSpec Output Path",
    driftspec_output_path_placeholder: "driftspec/generated/service_spec.yaml",
    build_driftspec_btn: "Build DriftSpec",
    build_spec_idle: "No DriftSpec has been generated yet.",
    run_driftspec_title: "Run DriftSpec",
    yaml_chip: "YAML",
    examples_label: "Examples",
    yaml_path_label: "YAML Path",
    yaml_path_placeholder: "driftspec/examples/demo_data_single.yaml",
    run_job_btn: "Run Job",
    repo_path_hint: "Paths must stay inside this repository. Relative paths are supported.",
    generate_chip: "Generate",
    trace_spec_title: "Trace → Spec",
    trace_path_label: "Trace Path",
    trace_path_placeholder: "driftspec/trace_inputs/trace_data_mock.csv",
    trace_output_yaml_label: "Output YAML Path",
    trace_output_yaml_placeholder: "driftspec/generated/trace_data_mock.yaml",
    trace_type_label: "Trace Type",
    trace_type_auto: "Auto detect",
    mapping_path_label: "Mapping Path (Optional)",
    mapping_path_placeholder: "driftspec/trace_inputs/mapping.json",
    generate_driftspec_btn: "Generate DriftSpec",
    no_jobs: "No jobs yet",
    jobs_title: "Jobs",
    logs_title: "Logs",
    logs_idle: "No job has started yet.",
    select_example: "Select an example...",
    load_failed: "Load failed",
    delete_btn: "Delete",
    delete_confirm: "Delete job #{jobId}?",
    job_deleted: "This job has been deleted.",
    delete_failed: "Delete failed: {error}",
    no_logs: "No logs yet.",
    log_trimmed: "[log trimmed: {count} lines]",
    executing: "Running...",
    schema_loading: "Loading",
    schema_updated: "Updated",
    failed_short: "Failed",
    read_schema_failed: "Failed to read schema",
    select_table_in_graph: "Click table nodes in the graph above to choose tables.",
    no_tables_to_show: "No tables to display.",
    graph_title_no_edges: "No relationship edges detected. Showing table and key information only. Nodes are draggable.",
    graph_title_fk: "Relationships from foreign key constraints. Nodes are draggable.",
    graph_title_inferred: "Relationships inferred from column names. Nodes are draggable.",
    graph_summary: "{title} · Edges: {count} · Click to multi-select, drag to reposition",
    graph_aria: "schema relation graph",
    more_count: "+{count} more",
    load_schema_first: "Load a schema first.",
    click_graph_select_multi: "Click table nodes in the graph above to select them. Multi-select is supported.",
    rows_short: "rows",
    cols_short: "cols",
    no_sample_rows: "No sample rows available.",
    no_column_info: "No column information available.",
    click_graph_show_schema: "Click a table node in the graph above to show sample rows and schema details here.",
    wait_sample_rows: "Waiting for sample rows...",
    loading_sample_rows: "Loading sample rows...",
    sample_rows_load_failed: "Failed to load sample rows: {error}",
    unknown_error: "unknown error",
    expand: "Expand",
    collapse: "Collapse",
    collapsed_hint: "This table is collapsed. Click \"Expand\" to view the first 5 rows and schema details.",
    top_rows: "Top 5 Rows",
    compact_schema_title: "Compact Schema",
    schema_empty: "Schema is empty",
    no_selectable_table: "No selectable tables",
    table_label: "Tables",
    column_label: "Columns",
    total_rows_label: "Total Rows",
    fk_edges_label: "FK/Edges",
    unable_load_logs: "Unable to load logs",
    enter_yaml_path: "Enter a YAML path",
    run_failed: "Run failed: {error}",
    enter_trace_and_output: "Enter both the trace path and output path",
    generate_failed: "Generation failed: {error}",
    temporal_drift_label: "Temporal Drift",
    temporal_generate_pattern: "Generate a time pattern for this drift",
    timestamp_column_label: "Timestamp Column",
    data_item_title: "Data #{index}",
    workload_item_title: "Workload #{index}",
    remove_btn: "Delete",
    name_optional: "Name (Optional)",
    drift_type_label: "Drift Type",
    data_drift_temporal_hint: "This drift is already temporal, so no extra temporal drift will be added.",
    columns_csv_label: "columns (comma separated)",
    baseline_hint: "Baseline uses default template parameters without extra drift parameters.",
    data_drift_csv_required: "Data Drift currently requires a CSV data source. Switch the Source Type to csv first.",
    data_drift_csv_path_required: "Data Drift requires data_source.path (CSV path).",
    add_one_data_drift: "Add at least one Data Drift item.",
    outlier_column_required: "outlier_injection requires a column.",
    add_one_workload_drift: "Add at least one Workload Drift item.",
    workload_csv_path_required: "Workload Drift (csv) requires a data path.",
    workload_postgres_config_required: "Workload Drift (postgres) requires a DB config path. Upload or save a config first.",
    unsupported_source_type: "Unsupported source type: {sourceType}",
    selectivity_column_required: "The selectivity drift type requires a column name.",
    distribution_column_required: "The distribution drift type requires a column name.",
    enable_one_drift_card: "Enable at least one drift card.",
    building_driftspec: "Building DriftSpec...",
    generated_paths: "Generated: {paths}",
    upload_or_paste_db_config: "Upload or paste a DB config JSON first.",
    saved_path: "Saved: {path}",
    save_db_config_failed: "Failed to save DB config: {error}",
    enter_data_path: "Enter a data path",
    schema_job_submitted: "The job has been submitted. Waiting for backend logs...",
    read_file_failed: "Failed to read file",
    select_config: "Select a config...",
    uploading: "Uploading...",
    saving: "Saving...",
    failed_with_error: "Failed: {error}",
    enter_csv_content: "Enter CSV content",
    enter_db_json: "Enter DB config JSON",
    select_schema_first: "Select a schema file first",
    no_visualizable_schema: "No schema file is available to visualize yet. Generate a Schema first.",
    build_failed: "Generation failed: {error}",
    job_label: "job",
  },
};

let currentJobId = null;
let pollTimer = null;
let lastSuggestedOutputPath = "";
let currentSchemaRaw = null;
let currentSchemaNorm = null;
let currentSchemaPath = "";
let selectedTableNames = [];
let primarySelectedTable = "";
const tablePreviewCache = {};
const tableDetailCollapsed = {};
const graphNodePositions = {};
let driftItemCounter = 1;
let dataDriftItems = [];
let workloadDriftItems = [];
let currentLang = "zh";

function interpolate(template, vars = {}) {
  return String(template).replace(/\{(\w+)\}/g, (_, key) => String(vars[key] ?? ""));
}

function t(key, vars) {
  const dict = I18N[currentLang] || I18N.zh;
  const fallback = I18N.zh[key] || key;
  return interpolate(dict[key] || fallback, vars);
}

function translateJobStatus(status) {
  const labels = {
    queued: currentLang === "en" ? "Queued" : "排队中",
    running: currentLang === "en" ? "Running" : "运行中",
    completed: currentLang === "en" ? "Completed" : "已完成",
    failed: currentLang === "en" ? "Failed" : "失败",
  };
  return labels[status] || status;
}

function detectInitialLanguage() {
  try {
    const saved = window.localStorage.getItem("driftbench_lang");
    if (saved === "zh" || saved === "en") return saved;
  } catch (_) {
    // no-op
  }
  const navLang = String(navigator.language || "").toLowerCase();
  return navLang.startsWith("zh") ? "zh" : "en";
}

function applyStaticTranslations() {
  document.documentElement.lang = currentLang === "zh" ? "zh-CN" : "en";
  document.title = t("page_title");
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.getAttribute("data-i18n");
    if (key) el.textContent = t(key);
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
    const key = el.getAttribute("data-i18n-placeholder");
    if (key && "placeholder" in el) {
      el.placeholder = t(key);
    }
  });
  document.querySelectorAll("[data-i18n-aria-label]").forEach((el) => {
    const key = el.getAttribute("data-i18n-aria-label");
    if (key) el.setAttribute("aria-label", t(key));
  });
  if (languageSelect) {
    languageSelect.value = currentLang;
    languageSelect.setAttribute("aria-label", t("language_label"));
  }
}

function renderDefaultPanels() {
  if (!currentSchemaRaw || !currentSchemaPath) {
    schemaPreviewTag.textContent = t("schema_preview_pending");
    schemaGraph.innerHTML = `<div class="empty">${t("schema_graph_empty")}</div>`;
    schemaOverview.innerHTML = `<div class="empty">${t("schema_overview_empty")}</div>`;
    schemaTableList.innerHTML = `<div class="empty">${t("schema_table_list_empty")}</div>`;
    schemaPreview.innerHTML = `<div class="empty">${t("schema_preview_empty")}</div>`;
  }
  if (!currentJobId) {
    logStatus.textContent = t("wait_task");
    logBox.textContent = t("logs_idle");
    schemaLiveStatus.textContent = t("wait_task");
    schemaLiveLog.textContent = t("schema_live_log_idle");
  }
}

function refreshLocalizedUi() {
  applyStaticTranslations();
  renderDefaultPanels();
  renderDataDriftItems();
  renderWorkloadDriftItems();
  renderDriftPaneState();
  if (currentSchemaRaw && currentSchemaPath) {
    renderSchemaPreview(currentSchemaRaw, currentSchemaPath);
  }
  if (currentJobId) {
    selectJob(currentJobId);
  } else {
    refreshJobs();
  }
  loadSpecs();
  loadSchemaFiles();
  if (schemaSourceType.value === "postgres") {
    loadDbConfigs();
  }
  checkHealth();
}

function setLanguage(lang, persist = true) {
  currentLang = lang === "en" ? "en" : "zh";
  if (persist) {
    try {
      window.localStorage.setItem("driftbench_lang", currentLang);
    } catch (_) {
      // no-op
    }
  }
  refreshLocalizedUi();
}

function initSideNavToggle() {
  if (!sideNavEl || !navToggleBtn) return;
  const key = "driftbench_side_nav_collapsed";
  let collapsed = false;
  try {
    collapsed = window.localStorage.getItem(key) === "1";
  } catch (_) {
    collapsed = false;
  }

  const apply = () => {
    sideNavEl.classList.toggle("collapsed", collapsed);
    navToggleBtn.setAttribute("aria-expanded", String(!collapsed));
    navToggleBtn.setAttribute("aria-label", collapsed ? t("nav_expand") : t("nav_collapse"));
  };

  navToggleBtn.addEventListener("click", () => {
    collapsed = !collapsed;
    apply();
    try {
      window.localStorage.setItem(key, collapsed ? "1" : "0");
    } catch (_) {
      // no-op
    }
  });

  apply();
}

function initSideNav() {
  if (sideNavLinks.length === 0) return;
  const targets = sideNavLinks
    .map((link) => {
      const href = link.getAttribute("href") || "";
      const id = href.startsWith("#") ? href.slice(1) : "";
      const el = id ? document.getElementById(id) : null;
      if (!el) return null;
      return { id, link, el };
    })
    .filter(Boolean);
  if (targets.length === 0) return;

  const setActive = (id) => {
    targets.forEach((item) => {
      item.link.classList.toggle("active", item.id === id);
    });
  };

  sideNavLinks.forEach((link) => {
    link.addEventListener("click", () => {
      const href = link.getAttribute("href") || "";
      if (href.startsWith("#")) {
        setActive(href.slice(1));
      }
    });
  });

  if (!("IntersectionObserver" in window)) {
    setActive(targets[0].id);
    return;
  }

  const observer = new IntersectionObserver((entries) => {
    const visible = entries
      .filter((entry) => entry.isIntersecting)
      .sort((a, b) => b.intersectionRatio - a.intersectionRatio);
    if (visible.length === 0) return;
    const id = visible[0].target.getAttribute("id");
    if (id) setActive(id);
  }, {
    root: null,
    rootMargin: "-28% 0px -58% 0px",
    threshold: [0.1, 0.25, 0.5, 0.75],
  });

  targets.forEach((item) => observer.observe(item.el));
  setActive(targets[0].id);
}

async function apiGet(path) {
  const res = await fetch(path, { headers: { "Accept": "application/json" } });
  if (!res.ok) {
    throw new Error(`GET ${path} failed: ${res.status}`);
  }
  return res.json();
}

async function apiPost(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `POST ${path} failed: ${res.status}`);
  }
  return res.json();
}

function setStatus(ok, message) {
  statusDot.style.background = ok ? "#2bb36e" : "#f0b429";
  statusDot.style.boxShadow = ok
    ? "0 0 12px rgba(43, 179, 110, 0.7)"
    : "0 0 12px rgba(240, 180, 41, 0.7)";
  statusText.textContent = message;
}

async function checkHealth() {
  try {
    await apiGet("/api/health");
    setStatus(true, t("status_connected"));
  } catch (err) {
    setStatus(false, t("status_disconnected"));
  }
}

async function loadSpecs() {
  try {
    const data = await apiGet("/api/specs");
    specSelect.innerHTML = "";
    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = t("select_example");
    specSelect.appendChild(empty);

    data.specs.forEach((spec) => {
      const opt = document.createElement("option");
      opt.value = spec.path;
      opt.textContent = spec.label;
      specSelect.appendChild(opt);
    });
  } catch (err) {
    specSelect.innerHTML = `<option value="">${t("load_failed")}</option>`;
  }
}

function renderJobs(jobs) {
  jobCount.textContent = jobs.length;
  jobsList.innerHTML = "";
  if (jobs.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = t("no_jobs");
    jobsList.appendChild(empty);
    return;
  }

  jobs.forEach((job) => {
    const item = document.createElement("div");
    item.className = "job-item";
    item.dataset.jobId = job.id;
    item.innerHTML = `
      <div class="job-meta">
        <div class="job-title">#${job.id} · ${job.kind}</div>
        <div class="job-sub">${translateJobStatus(job.status)} · ${job.created_at}</div>
      </div>
      <div class="job-actions">
        <span class="status-pill status-${job.status}">${translateJobStatus(job.status)}</span>
        <button class="job-delete">${t("delete_btn")}</button>
      </div>
    `;
    item.addEventListener("click", () => {
      selectJob(job.id);
    });
    const deleteBtn = item.querySelector(".job-delete");
    deleteBtn.addEventListener("click", async (event) => {
      event.stopPropagation();
      await deleteJob(job.id);
    });
    jobsList.appendChild(item);
  });
}

async function deleteJob(jobId) {
  const ok = confirm(t("delete_confirm", { jobId }));
  if (!ok) return;
  try {
    await apiPost("/api/jobs/delete", { job_id: jobId });
    if (currentJobId === jobId) {
      currentJobId = null;
      logStatus.textContent = t("wait_task");
      logBox.textContent = t("job_deleted");
      schemaLiveStatus.textContent = t("wait_task");
      schemaLiveLog.textContent = t("job_deleted");
    }
    await refreshJobs();
  } catch (err) {
    alert(t("delete_failed", { error: err.message }));
  }
}

function renderLogs(job) {
  currentJobId = job.id;
  const status = job.status;
  logStatus.textContent = `${translateJobStatus(status)} · ${t("job_label")} #${job.id}`;
  const lines = job.logs || [];
  const dropped = job.log_dropped || 0;
  let text = "";
  if (dropped > 0) {
    text += `${t("log_trimmed", { count: dropped })}\n`;
  }
  text += lines.join("\n");
  logBox.textContent = text || t("no_logs");
  logBox.scrollTop = logBox.scrollHeight;
  renderSchemaLiveLog(job);

  if (job.kind === "schema-extract" && job.status === "completed" && job.result?.schema_path) {
    schemaFileSelect.value = job.result.schema_path;
    loadSchemaFiles();
    loadSchemaPreview(job.result.schema_path);
  }
}

function renderSchemaLiveLog(job) {
  if (job.kind !== "schema-extract") {
    return;
  }
  const lines = job.logs || [];
  const dropped = job.log_dropped || 0;
  let text = "";
  if (dropped > 0) {
    text += `${t("log_trimmed", { count: dropped })}\n`;
  }
  text += lines.join("\n");
  schemaLiveStatus.textContent = translateJobStatus(job.status || "running");
  schemaLiveLog.textContent = text || t("executing");
  schemaLiveLog.scrollTop = schemaLiveLog.scrollHeight;
}

async function refreshJobs() {
  try {
    const data = await apiGet("/api/jobs");
    renderJobs(data.jobs || []);
  } catch (err) {
    jobCount.textContent = "0";
  }
}

async function loadSchemaPreview(path) {
  if (!path) return;
  schemaPreviewTag.textContent = t("schema_loading");
  try {
    const data = await apiGet(`/api/schema/read?path=${encodeURIComponent(path)}`);
    renderSchemaPreview(data.schema, data.path);
    schemaPreviewTag.textContent = t("schema_updated");
  } catch (err) {
    schemaPreviewTag.textContent = t("failed_short");
    currentSchemaRaw = null;
    currentSchemaNorm = null;
    currentSchemaPath = "";
    selectedTableNames = [];
    primarySelectedTable = "";
    schemaGraph.innerHTML = `<div class="empty">${t("read_schema_failed")}</div>`;
    schemaOverview.innerHTML = `<div class="empty">${t("read_schema_failed")}</div>`;
    schemaTableList.innerHTML = `<div class="empty">${t("select_table_in_graph")}</div>`;
    schemaPreview.innerHTML = `<div class="empty">${t("read_schema_failed")}</div>`;
  }
}

async function loadSchemaFiles() {
  try {
    const data = await apiGet("/api/schemas");
    const files = data.files || [];
    const previous = schemaFileSelect.value;
    schemaFileSelect.innerHTML = "";
    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = t("select_schema_file");
    schemaFileSelect.appendChild(empty);
    files.forEach((file) => {
      const opt = document.createElement("option");
      opt.value = file;
      opt.textContent = file.split("/").pop();
      schemaFileSelect.appendChild(opt);
    });

    if (previous && files.includes(previous)) {
      schemaFileSelect.value = previous;
    } else if (!previous && files.length > 0) {
      schemaFileSelect.value = files[0];
      await loadSchemaPreview(files[0]);
    }
  } catch (err) {
    schemaFileSelect.innerHTML = `<option value="">${t("load_failed")}</option>`;
  }
}

function normalizeSchema(raw) {
  if (!raw) return { tables: {} };
  if (raw.tables) return raw;
  if (raw.source && raw.source.columns) {
    return {
      tables: {
        [raw.source.table || "table"]: {
          columns: raw.source.columns,
          num_rows: raw.num_rows || 0,
        },
      },
    };
  }
  return { tables: {} };
}

function summarizeTypes(columns) {
  const counts = {};
  Object.values(columns).forEach((info) => {
    const t = info.logical_type || "unknown";
    counts[t] = (counts[t] || 0) + 1;
  });
  return counts;
}

function renderTypeBars(counts) {
  const total = Object.values(counts).reduce((a, b) => a + b, 0) || 1;
  return Object.entries(counts)
    .map(([key, value]) => {
      const pct = Math.round((value / total) * 100);
      return `
        <div class="type-bar">
          <div>${key}</div>
          <div class="bar"><span style="width:${pct}%"></span></div>
          <div>${value}</div>
        </div>
      `;
    })
    .join("");
}

function isKeyLikeColumn(colName, normalized = false) {
  const c = String(colName || "").toLowerCase();
  if (!c) return false;
  if (normalized) {
    return c === "id" || c.endsWith("id") || c.endsWith("sk") || c.endsWith("key");
  }
  return c.endsWith("_id") || c.endsWith("_sk") || c.endsWith("_key") || c === "id";
}

function normalizeKeyByTableInitial(tableName, colName) {
  const bare = String(tableName || "").split(".").pop().toLowerCase().replace(/[^a-z0-9_]/g, "");
  let col = String(colName || "").toLowerCase().replace(/[^a-z0-9_]/g, "");
  if (!col) return "";
  const initial = bare.charAt(0);
  if (initial && col.startsWith(`${initial}_`)) {
    col = col.slice(2);
  }
  return col.replace(/^_+/, "");
}

function keyPkScore(colInfo, colName, rowCount) {
  const uniq = Number(colInfo?.num_unique);
  let score = 0;
  if (Number.isFinite(uniq) && rowCount > 0) {
    score += Math.min(1, uniq / rowCount);
  }
  if (isKeyLikeColumn(colName)) {
    score += 0.2;
  }
  return score;
}

function inferRelationshipsLegacy(schema) {
  const tableNames = Object.keys(schema.tables || {});
  const edges = [];
  const seen = new Set();
  for (const src of tableNames) {
    const cols = Object.keys(schema.tables[src]?.columns || {});
    for (const col of cols) {
      const colLower = col.toLowerCase();
      if (!colLower.endsWith("_id") && !colLower.endsWith("_sk") && !colLower.endsWith("_key")) {
        continue;
      }
      for (const dst of tableNames) {
        if (src === dst) continue;
        const bare = dst.split(".").pop().toLowerCase();
        const tokens = bare.split("_").filter((t) => t.length >= 3);
        const matched = colLower.includes(bare) || tokens.some((t) => colLower.includes(t));
        if (!matched) continue;
        const key = `${src}|${dst}|${colLower}`;
        if (seen.has(key)) continue;
        seen.add(key);
        edges.push({
          source_table: src,
          target_table: dst,
          source_column: col,
          target_column: "",
          inferred: true,
          inferred_rule: "legacy_name_match",
        });
      }
    }
  }
  return edges;
}

function inferRelationships(schema) {
  const tables = schema.tables || {};
  const groups = {};
  Object.entries(tables).forEach(([tableName, table]) => {
    const cols = table?.columns || {};
    const rowCount = Number(table?.num_rows || 0);
    Object.entries(cols).forEach(([colName, colInfo]) => {
      const keyName = normalizeKeyByTableInitial(tableName, colName);
      if (!isKeyLikeColumn(keyName, true)) return;
      if (!groups[keyName]) groups[keyName] = [];
      groups[keyName].push({
        table: tableName,
        column: colName,
        numUnique: Number(colInfo?.num_unique),
        score: keyPkScore(colInfo, colName, rowCount),
      });
    });
  });

  const edges = [];
  const seen = new Set();
  Object.entries(groups).forEach(([keyName, members]) => {
    if (!Array.isArray(members) || members.length < 2) return;
    const ranked = [...members].sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const aUniq = Number.isFinite(a.numUnique) ? a.numUnique : -1;
      const bUniq = Number.isFinite(b.numUnique) ? b.numUnique : -1;
      if (bUniq !== aUniq) return bUniq - aUniq;
      return a.table.localeCompare(b.table);
    });
    const parent = ranked[0];
    ranked.slice(1).forEach((child) => {
      if (child.table === parent.table) return;
      const dedup = `${child.table}|${parent.table}|${child.column}|${parent.column}`;
      if (seen.has(dedup)) return;
      seen.add(dedup);
      edges.push({
        source_table: child.table,
        source_column: child.column,
        target_table: parent.table,
        target_column: parent.column,
        inferred: true,
        inferred_rule: "table_initial_prefix",
        inferred_key: keyName,
      });
    });
  });
  if (edges.length > 0) {
    return edges;
  }
  return inferRelationshipsLegacy(schema);
}

function getSchemaRelationships(raw, schema) {
  if (Array.isArray(raw.relationships) && raw.relationships.length > 0) {
    return { edges: raw.relationships, mode: "fk" };
  }
  return { edges: inferRelationships(schema), mode: "inferred" };
}

function esc(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function getTableKeyInfo(raw, schema, edges) {
  const info = {};
  const tableNames = Object.keys(schema.tables || {});
  tableNames.forEach((name) => {
    const t = (raw.tables && raw.tables[name]) || {};
    let pks = Array.isArray(t.primary_keys) ? [...t.primary_keys] : [];
    let fks = [];
    if (pks.length === 0) {
      pks = Array.from(
        new Set(
          edges
            .filter((e) => e.target_table === name && e.target_column)
            .map((e) => e.target_column),
        ),
      );
    }

    if (Array.isArray(t.foreign_keys) && t.foreign_keys.length > 0) {
      fks = t.foreign_keys.map((fk) => `${fk.column} -> ${fk.target_table.split(".").pop()}.${fk.target_column}`);
    } else {
      fks = edges
        .filter((e) => e.source_table === name)
        .map((e) => `${e.source_column} -> ${e.target_table.split(".").pop()}.${e.target_column || "?"}`);
    }
    info[name] = { pks, fks };
  });
  return info;
}

function compactList(values, max = 2) {
  const arr = Array.from(new Set(values || []));
  if (arr.length <= max) return arr;
  return [...arr.slice(0, max), t("more_count", { count: arr.length - max })];
}

function renderSchemaGraph(raw, schema) {
  const tableNames = Object.keys(schema.tables || {});
  if (tableNames.length === 0) {
    schemaGraph.innerHTML = `<div class="empty">${t("no_tables_to_show")}</div>`;
    return;
  }

  const relation = getSchemaRelationships(raw, schema);
  const edges = relation.edges || [];
  const keyInfo = getTableKeyInfo(raw, schema, edges);
  const sorted = [...tableNames].sort();
  const nodeW = 250;
  const gapX = 42;
  const gapY = 34;
  const margin = 18;
  const lineH = 14;
  const headerY = 22;
  const cols = Math.max(3, Math.ceil(Math.sqrt(sorted.length * 1.4)));

  const nodes = sorted.map((name, idx) => {
    const info = keyInfo[name] || { pks: [], fks: [] };
    const pks = compactList(info.pks, 4);
    const fks = compactList(info.fks, 4);
    const pkLines = pks.length ? pks : ["-"];
    const fkLines = fks.length ? fks : ["-"];
    const nodeH = 18 + 16 + pkLines.length * lineH + 16 + fkLines.length * lineH + 14;

    const row = Math.floor(idx / cols);
    const col = idx % cols;
    const defaultX = margin + col * (nodeW + gapX);
    const defaultY = 56 + margin + row * (150 + gapY);
    const cached = graphNodePositions[name];
    return {
      name,
      pks: pkLines,
      fks: fkLines,
      w: nodeW,
      h: nodeH,
      x: cached ? cached.x : defaultX,
      y: cached ? cached.y : defaultY,
    };
  });

  const rows = Math.ceil(sorted.length / cols);
  const W = margin * 2 + cols * nodeW + (cols - 1) * gapX;
  const H = 68 + margin * 2 + rows * 160;

  const toPoint = (svg, clientX, clientY) => {
    const pt = svg.createSVGPoint();
    pt.x = clientX;
    pt.y = clientY;
    const inv = svg.getScreenCTM().inverse();
    return pt.matrixTransform(inv);
  };

  const nodeByName = () => {
    const m = {};
    nodes.forEach((n) => { m[n.name] = n; });
    return m;
  };

  const edgePath = (s, t) => {
    const leftToRight = s.x <= t.x;
    const x1 = leftToRight ? s.x + s.w : s.x;
    const y1 = s.y + s.h / 2;
    const x2 = leftToRight ? t.x : t.x + t.w;
    const y2 = t.y + t.h / 2;
    const dx = Math.max(44, Math.min(180, Math.abs(x2 - x1) * 0.45));
    const c1x = leftToRight ? x1 + dx : x1 - dx;
    const c2x = leftToRight ? x2 - dx : x2 + dx;
    return `M ${x1} ${y1} C ${c1x} ${y1}, ${c2x} ${y2}, ${x2} ${y2}`;
  };

  const title = edges.length === 0
    ? t("graph_title_no_edges")
    : (relation.mode === "fk" ? t("graph_title_fk") : t("graph_title_inferred"));

  const draw = () => {
    const map = nodeByName();
    const edgeSvg = edges
      .map((e) => {
        const s = map[e.source_table];
        const t = map[e.target_table];
        if (!s || !t) return "";
        return `<path d="${edgePath(s, t)}" fill="none" stroke="rgba(44,107,255,0.34)" stroke-width="1.5" marker-end="url(#arrow)"></path>`;
      })
      .join("");

    const nodeSvg = nodes
      .map((n, idx) => {
        const label = n.name.split(".").pop();
        const tableInfo = schema.tables[n.name] || {};
        const selected = selectedTableNames.includes(n.name);
        const pkRows = n.pks
          .map((k, i) => `<text x="${n.x + 10}" y="${n.y + headerY + 18 + (i * lineH)}" font-size="11" fill="#2c6bff">${esc(k)}</text>`)
          .join("");
        const fkStart = n.y + headerY + 18 + (n.pks.length * lineH) + 16;
        const fkRows = n.fks
          .map((k, i) => `<text x="${n.x + 10}" y="${fkStart + (i * lineH)}" font-size="11" fill="#ff6b2c">${esc(k)}</text>`)
          .join("");
        return `
          <g data-node-idx="${idx}" class="schema-node ${selected ? "selected" : ""}" data-table-name="${esc(n.name)}">
            <rect x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" rx="12" fill="${selected ? "#eef4ff" : "#ffffff"}" stroke="${selected ? "rgba(44,107,255,0.72)" : "rgba(28,28,28,0.24)"}"></rect>
            <text x="${n.x + 10}" y="${n.y + 18}" font-size="12" font-weight="600" fill="#1c1c1c">${esc(label)}</text>
            <text x="${n.x + 10}" y="${n.y + 31}" font-size="10" fill="#666">${t("rows_short")}:${tableInfo.num_rows ?? "-"} ${t("cols_short")}:${Object.keys(tableInfo.columns || {}).length}</text>
            <text x="${n.x + 10}" y="${n.y + headerY}" font-size="11" fill="#2c6bff">PK</text>
            ${pkRows}
            <text x="${n.x + 10}" y="${fkStart - 6}" font-size="11" fill="#ff6b2c">FK</text>
            ${fkRows}
          </g>
        `;
      })
      .join("");

    schemaGraph.innerHTML = `
      <svg id="schemaGraphSvg" viewBox="0 0 ${W} ${H}" role="img" aria-label="${escAttr(t("graph_aria"))}">
        <defs>
          <marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="4.5" orient="auto">
            <path d="M0,0 L9,4.5 L0,9 Z" fill="rgba(44,107,255,0.55)"></path>
          </marker>
        </defs>
        <text x="16" y="24" font-size="13" fill="#5a5a5a">${esc(t("graph_summary", { title, count: edges.length }))}</text>
        ${edgeSvg}
        ${nodeSvg}
      </svg>
    `;
  };

  let draggingIdx = -1;
  let dragDx = 0;
  let dragDy = 0;
  let dragStartX = 0;
  let dragStartY = 0;
  let dragMoved = false;

  const onMove = (evt) => {
    if (draggingIdx < 0) return;
    const svg = schemaGraph.querySelector("#schemaGraphSvg");
    if (!svg) return;
    const dist = Math.hypot(evt.clientX - dragStartX, evt.clientY - dragStartY);
    if (dist > 4) {
      dragMoved = true;
    }
    if (!dragMoved) return;
    const p = toPoint(svg, evt.clientX, evt.clientY);
    const n = nodes[draggingIdx];
    n.x = Math.max(8, Math.min(W - n.w - 8, p.x - dragDx));
    n.y = Math.max(42, Math.min(H - n.h - 8, p.y - dragDy));
    graphNodePositions[n.name] = { x: n.x, y: n.y };
    draw();
    bindMouseDown();
  };

  const onUp = () => {
    if (draggingIdx >= 0 && !dragMoved) {
      toggleSelectedTable(nodes[draggingIdx].name);
    }
    draggingIdx = -1;
    dragMoved = false;
    window.removeEventListener("mousemove", onMove);
    window.removeEventListener("mouseup", onUp);
  };

  const onMouseDown = (evt) => {
    const node = evt.target.closest("g[data-node-idx]");
    if (!node) return;
    const idx = Number(node.getAttribute("data-node-idx"));
    if (Number.isNaN(idx)) return;
    const svg = schemaGraph.querySelector("#schemaGraphSvg");
    const p = toPoint(svg, evt.clientX, evt.clientY);
    draggingIdx = idx;
    dragDx = p.x - nodes[idx].x;
    dragDy = p.y - nodes[idx].y;
    dragStartX = evt.clientX;
    dragStartY = evt.clientY;
    dragMoved = false;
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  };

  const bindMouseDown = () => {
    const svg = schemaGraph.querySelector("#schemaGraphSvg");
    if (!svg) return;
    svg.onmousedown = onMouseDown;
  };

  draw();
  bindMouseDown();
}

function makePreviewCacheKey(tableName) {
  return `${currentSchemaPath}::${tableName}`;
}

function clearPreviewCacheForSchema(schemaPath) {
  const prefix = `${schemaPath}::`;
  Object.keys(tablePreviewCache).forEach((key) => {
    if (!key.startsWith(prefix)) {
      delete tablePreviewCache[key];
    }
  });
}

function toggleSelectedTable(tableName) {
  if (!currentSchemaNorm?.tables?.[tableName]) return;
  if (selectedTableNames.includes(tableName)) {
    selectedTableNames = selectedTableNames.filter((name) => name !== tableName);
  } else {
    selectedTableNames = [...selectedTableNames, tableName];
  }
  primarySelectedTable = selectedTableNames[0] || "";
  renderSchemaGraph(currentSchemaRaw || {}, currentSchemaNorm);
  renderSelectedTableSummary();
  renderSelectedTablesDetails();
  fetchPreviewsForSelectedTables();
}

function renderSelectedTableSummary() {
  if (!currentSchemaNorm) {
    schemaTableList.innerHTML = `<div class="empty">${t("load_schema_first")}</div>`;
    return;
  }
  if (selectedTableNames.length === 0) {
    schemaTableList.innerHTML = `<div class="empty">${t("click_graph_select_multi")}</div>`;
    return;
  }
  schemaTableList.innerHTML = selectedTableNames
    .map((name) => {
      const table = currentSchemaNorm.tables[name] || {};
      const cols = Object.keys(table.columns || {}).length;
      const rows = table.num_rows ?? "-";
      return `<span class="selected-table-chip" data-table-name="${esc(name)}">${esc(name)} · ${t("rows_short")}:${rows} ${t("cols_short")}:${cols}</span>`;
    })
    .join("");
}

function renderRowsTable(columns, rows) {
  const safeRows = (rows || []).slice(0, 5);
  if (safeRows.length === 0) {
    return `<div class="empty">${t("no_sample_rows")}</div>`;
  }
  const cols = (columns && columns.length) ? columns : Object.keys(safeRows[0] || {});
  const header = cols.map((c) => `<th>${esc(c)}</th>`).join("");
  const body = safeRows
    .map((row) => {
      const cells = cols.map((col) => {
        const val = row[col];
        return `<td>${esc(val === null || val === undefined ? "" : String(val))}</td>`;
      }).join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  return `
    <div class="sample-table-wrap">
      <table class="sample-table">
        <thead><tr>${header}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function renderSchemaColumnsCompact(columns, pkSet, fkSet) {
  const entries = Object.entries(columns || {});
  if (entries.length === 0) {
    return `<div class="empty">${t("no_column_info")}</div>`;
  }
  const body = entries
    .map(([colName, info]) => {
      const logicalType = info.logical_type || "-";
      const uniq = info.num_unique !== undefined ? String(info.num_unique) : "-";
      const rangeText = (info.range && info.range.min !== undefined)
        ? `${info.range.min} ~ ${info.range.max}`
        : "-";
      const keyTags = [];
      if (pkSet.has(colName)) keyTags.push("PK");
      if (fkSet.has(colName)) keyTags.push("FK");
      return `
        <tr>
          <td>${esc(colName)}</td>
          <td>${esc(logicalType)}</td>
          <td>${esc(uniq)}</td>
          <td>${esc(rangeText)}</td>
          <td>${keyTags.length ? esc(keyTags.join(",")) : "-"}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <div class="schema-col-table-wrap">
      <table class="schema-col-table">
        <thead>
          <tr>
            <th>column</th>
            <th>type</th>
            <th>unique</th>
            <th>range</th>
            <th>key</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function renderSelectedTablesDetails() {
  if (!currentSchemaNorm) {
    schemaPreview.innerHTML = `<div class="empty">${t("load_schema_first")}</div>`;
    return;
  }
  if (selectedTableNames.length === 0) {
    schemaPreview.innerHTML = `<div class="empty">${t("click_graph_show_schema")}</div>`;
    return;
  }

  const html = selectedTableNames
    .map((tableName) => {
      const table = currentSchemaNorm.tables[tableName] || {};
      const cols = table.columns || {};
      const typeBars = renderTypeBars(summarizeTypes(cols));
      const pks = table.primary_keys || [];
      let fks = [];
      let fkCols = [];
      if (Array.isArray(table.foreign_keys) && table.foreign_keys.length > 0) {
        fks = table.foreign_keys.map((fk) => `${fk.column} -> ${fk.target_table}.${fk.target_column}`);
        fkCols = table.foreign_keys.map((fk) => fk.column);
      } else if (Array.isArray(currentSchemaRaw?.relationships)) {
        fks = currentSchemaRaw.relationships
          .filter((r) => r.source_table === tableName)
          .map((r) => `${r.source_column} -> ${r.target_table}.${r.target_column}`);
        fkCols = currentSchemaRaw.relationships
          .filter((r) => r.source_table === tableName)
          .map((r) => r.source_column);
      }
      const pkSet = new Set(pks || []);
      const fkSet = new Set(fkCols || []);
      const collapsed = !!tableDetailCollapsed[tableName];

      const key = makePreviewCacheKey(tableName);
      const preview = tablePreviewCache[key];
      let rowsPart = `<div class="empty">${t("wait_sample_rows")}</div>`;
      if (preview?.status === "loading") {
        rowsPart = `<div class="empty">${t("loading_sample_rows")}</div>`;
      } else if (preview?.status === "error") {
        rowsPart = `<div class="empty">${esc(t("sample_rows_load_failed", { error: preview.error || t("unknown_error") }))}</div>`;
      } else if (preview?.status === "ready") {
        rowsPart = renderRowsTable(preview.data.columns, preview.data.rows);
      }

      return `
        <div class="table-block">
          <div class="table-header">
            <div>${esc(tableName)}</div>
            <div class="inline-row">
              <div class="table-meta">${t("rows_short")}: ${table.num_rows ?? "-"} · ${t("cols_short")}: ${Object.keys(cols).length}</div>
              <button class="ghost mini-btn table-collapse-btn" type="button" data-action="toggle-table" data-table-name="${esc(tableName)}">${collapsed ? t("expand") : t("collapse")}</button>
            </div>
          </div>
          <div class="table-meta"><strong>PK:</strong> ${pks.length ? esc(pks.join(", ")) : "-"}</div>
          <div class="table-meta"><strong>FK:</strong> ${fks.length ? esc(fks.join(" ; ")) : "-"}</div>
          ${collapsed ? `
            <div class="table-meta">${t("collapsed_hint")}</div>
          ` : `
            <div class="type-bars">${typeBars}</div>
            <div class="table-subtitle">${t("top_rows")}</div>
            <div class="drag-scroll-x">${rowsPart}</div>
            <div class="table-subtitle">${t("compact_schema_title")}</div>
            <div class="drag-scroll-x">${renderSchemaColumnsCompact(cols, pkSet, fkSet)}</div>
          `}
        </div>
      `;
    })
    .join("");
  schemaPreview.innerHTML = html;
  initHorizontalDragScroll();
}

function initHorizontalDragScroll() {
  const items = schemaPreview.querySelectorAll(".drag-scroll-x");
  items.forEach((el) => {
    if (el.dataset.dragInit === "1") return;
    el.dataset.dragInit = "1";
    let dragging = false;
    let startX = 0;
    let startScrollLeft = 0;
    let pointerId = null;

    el.addEventListener("pointerdown", (evt) => {
      if (evt.pointerType === "mouse" && evt.button !== 0) return;
      dragging = true;
      pointerId = evt.pointerId;
      startX = evt.clientX;
      startScrollLeft = el.scrollLeft;
      el.classList.add("dragging");
      el.setPointerCapture(pointerId);
      evt.preventDefault();
    });

    el.addEventListener("pointermove", (evt) => {
      if (!dragging) return;
      const dx = evt.clientX - startX;
      el.scrollLeft = startScrollLeft - dx;
    });

    const stopDrag = () => {
      dragging = false;
      el.classList.remove("dragging");
      if (pointerId !== null) {
        try {
          el.releasePointerCapture(pointerId);
        } catch (_) {
          // no-op
        }
      }
      pointerId = null;
    };
    el.addEventListener("pointerup", stopDrag);
    el.addEventListener("pointercancel", stopDrag);
    el.addEventListener("pointerleave", () => {
      if (dragging && pointerId === null) stopDrag();
    });
  });
}

async function fetchTablePreview(tableName) {
  if (!currentSchemaPath) return;
  const key = makePreviewCacheKey(tableName);
  if (tablePreviewCache[key]?.status === "ready" || tablePreviewCache[key]?.status === "loading") return;
  tablePreviewCache[key] = { status: "loading" };
  renderSelectedTablesDetails();

  const payload = {
    schema_path: currentSchemaPath,
    table_name: tableName,
    limit: 5,
  };
  const sourceType = currentSchemaRaw?._meta?.source_type || schemaSourceType.value;
  if (sourceType) payload.source_type = sourceType;
  if (sourceType === "csv" && schemaDataPath.value.trim()) {
    payload.path = schemaDataPath.value.trim();
  }
  if (sourceType === "postgres") {
    if (schemaDbConfigPath.value.trim()) payload.db_config_path = schemaDbConfigPath.value.trim();
    if (schemaName.value.trim()) payload.schema_name = schemaName.value.trim();
  }

  try {
    const data = await apiPost("/api/schema/table-preview", payload);
    tablePreviewCache[key] = { status: "ready", data };
  } catch (err) {
    tablePreviewCache[key] = { status: "error", error: err.message };
  }
  renderSelectedTablesDetails();
}

function fetchPreviewsForSelectedTables() {
  selectedTableNames.forEach((tableName) => {
    fetchTablePreview(tableName);
  });
}

function renderSchemaPreview(raw, path) {
  const schema = normalizeSchema(raw);
  currentSchemaRaw = raw;
  currentSchemaNorm = schema;
  currentSchemaPath = path;
  clearPreviewCacheForSchema(path);
  const tableEntries = Object.entries(schema.tables || {});
  if (tableEntries.length === 0) {
    schemaGraph.innerHTML = `<div class="empty">${t("schema_empty")}</div>`;
    schemaOverview.innerHTML = `<div class="empty">${t("schema_empty")}</div>`;
    schemaTableList.innerHTML = `<div class="empty">${t("no_selectable_table")}</div>`;
    schemaPreview.innerHTML = `<div class="empty">${t("schema_empty")}</div>`;
    return;
  }
  const validNames = new Set(tableEntries.map(([name]) => name));
  selectedTableNames = selectedTableNames.filter((name) => validNames.has(name));
  if (selectedTableNames.length === 0) {
    selectedTableNames = [tableEntries[0][0]];
  }
  primarySelectedTable = selectedTableNames[0] || "";
  renderSchemaGraph(raw, schema);
  renderSchemaOverview(raw, schema, path);
  renderSelectedTableSummary();
  renderSelectedTablesDetails();
  fetchPreviewsForSelectedTables();
}

function renderSchemaOverview(raw, schema, path) {
  const tableEntries = Object.entries(schema.tables || {});
  const totalTables = tableEntries.length;
  const totalColumns = tableEntries.reduce((acc, [, t]) => acc + Object.keys(t.columns || {}).length, 0);
  const totalRows = tableEntries.reduce((acc, [, t]) => acc + Number(t.num_rows || 0), 0);
  const pks = tableEntries.reduce((acc, [, t]) => acc + ((t.primary_keys || []).length), 0);
  const fks = tableEntries.reduce((acc, [, t]) => acc + ((t.foreign_keys || []).length), 0);
  const relationInfo = getSchemaRelationships(raw, schema);
  schemaOverview.innerHTML = `
    <div class="schema-meta-bar">
      <span class="schema-meta-item"><strong>Schema:</strong> ${esc(path)}</span>
      <span class="schema-meta-item"><strong>${t("table_label")}:</strong> ${totalTables}</span>
      <span class="schema-meta-item"><strong>${t("column_label")}:</strong> ${totalColumns}</span>
      <span class="schema-meta-item"><strong>${t("total_rows_label")}:</strong> ${totalRows}</span>
      <span class="schema-meta-item"><strong>PK:</strong> ${pks}</span>
      <span class="schema-meta-item"><strong>${t("fk_edges_label")}:</strong> ${fks}/${relationInfo.edges.length}</span>
    </div>
  `;
}

async function selectJob(jobId) {
  if (!jobId) return;
  try {
    const data = await apiGet(`/api/jobs/${jobId}`);
    renderLogs(data.job);
    startPolling(jobId);
  } catch (err) {
    logBox.textContent = t("unable_load_logs");
  }
}

function startPolling(jobId) {
  if (pollTimer) {
    clearInterval(pollTimer);
  }
  pollTimer = setInterval(async () => {
    try {
      const data = await apiGet(`/api/jobs/${jobId}`);
      renderLogs(data.job);
      if (["completed", "failed"].includes(data.job.status)) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    } catch (err) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }, 1000);
}

async function runSpec() {
  const path = specPath.value.trim();
  if (!path) {
    alert(t("enter_yaml_path"));
    return;
  }
  try {
    const data = await apiPost("/api/run", { spec_path: path });
    await refreshJobs();
    renderLogs(data.job);
    startPolling(data.job.id);
  } catch (err) {
    alert(t("run_failed", { error: err.message }));
  }
}

async function runTrace() {
  const trace = tracePath.value.trim();
  const output = outputPath.value.trim();
  const type = traceType.value;
  const mapping = mappingPath.value.trim();

  if (!trace || !output) {
    alert(t("enter_trace_and_output"));
    return;
  }

  try {
    const payload = {
      trace_path: trace,
      output_path: output,
    };
    if (type) payload.trace_type = type;
    if (mapping) payload.mapping_path = mapping;

    const data = await apiPost("/api/trace-to-spec", payload);
    await refreshJobs();
    renderLogs(data.job);
    startPolling(data.job.id);
  } catch (err) {
    alert(t("generate_failed", { error: err.message }));
  }
}

function updateSchemaFields() {
  const type = schemaSourceType.value;
  const isDb = type === "postgres";
  schemaPathRow.style.display = isDb ? "none" : "flex";
  schemaDbConfigRow.style.display = isDb ? "flex" : "none";
  schemaNameRow.style.display = isDb ? "flex" : "none";
  schemaCsvExtras.style.display = isDb ? "none" : "block";
  schemaDbExtras.style.display = isDb ? "block" : "none";
  if (isDb) {
    loadDbConfigs();
  }
  if (isDb) {
    driftDataEnabled.checked = false;
    if (!driftWorkloadEnabled.checked) {
      driftWorkloadEnabled.checked = true;
    }
  } else if (!driftDataEnabled.checked && !driftWorkloadEnabled.checked) {
    driftDataEnabled.checked = true;
  }
  renderDriftPaneState();
  maybeSetSchemaOutputPath();
}

function basenameNoExt(path) {
  const raw = (path || "").trim();
  if (!raw) return "";
  const seg = raw.split("/").pop() || "";
  const idx = seg.lastIndexOf(".");
  return idx > 0 ? seg.slice(0, idx) : seg;
}

function sanitizeName(name) {
  return (name || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "") || "schema";
}

function buildSuggestedSchemaOutputPath() {
  const sourceType = schemaSourceType.value;
  if (sourceType === "postgres") {
    const schemaPart = sanitizeName(schemaName.value || "public");
    return `driftbench_service/schemas/${schemaPart}_schema.json`;
  }
  const stem = basenameNoExt(schemaDataPath.value) || "dataset";
  return `driftbench_service/schemas/${sanitizeName(stem)}_schema.json`;
}

function maybeSetSchemaOutputPath(force = false) {
  const suggested = buildSuggestedSchemaOutputPath();
  const current = schemaOutputPath.value.trim();
  const shouldReplace = force || !current || current === lastSuggestedOutputPath;
  if (shouldReplace) {
    schemaOutputPath.value = suggested;
  }
  lastSuggestedOutputPath = suggested;
}

function escAttr(text) {
  return esc(text).replaceAll("\"", "&quot;");
}

function parseNum(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseIntNum(value, fallback) {
  const n = Number.parseInt(String(value), 10);
  return Number.isFinite(n) ? n : fallback;
}

function splitColumns(raw) {
  return String(raw || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function defaultBaseTable() {
  if (primarySelectedTable) return primarySelectedTable;
  if (selectedTableNames.length > 0) return selectedTableNames[0];
  const names = Object.keys(currentSchemaNorm?.tables || {});
  if (names.length > 0) return names[0];
  if (schemaSourceType.value === "csv") {
    return basenameNoExt(schemaDataPath.value) || "table";
  }
  return "public.table";
}

function firstColumnForTable(tableName) {
  if (!tableName || !currentSchemaNorm?.tables?.[tableName]?.columns) return "";
  const cols = Object.keys(currentSchemaNorm.tables[tableName].columns || {});
  return cols[0] || "";
}

function getSchemaPathForSpec() {
  const picked = schemaFileSelect.value.trim();
  if (picked) return picked;
  const out = schemaOutputPath.value.trim();
  if (out) return out;
  return buildSuggestedSchemaOutputPath();
}

function makeDataDriftItem() {
  const defaultCol = firstColumnForTable(defaultBaseTable());
  return {
    id: `d_${driftItemCounter++}`,
    name: "",
    drift_type: "value_skew",
    columns: defaultCol ? `${defaultCol}` : "",
    portion: "1.0",
    skewness: "2",
    scale: "1.2",
    n: "1000",
    outlier_column: defaultCol,
    timestamp_column: "timestamp",
    temporal_enabled: false,
    temporal_pattern: "uniform",
    temporal_start: "2025-07-01T00:00:00",
    temporal_qpm: "300",
    temporal_column: "timestamp",
  };
}

function makeWorkloadDriftItem() {
  const defaultCol = firstColumnForTable(defaultBaseTable());
  return {
    id: `w_${driftItemCounter++}`,
    name: "",
    drift_type: "baseline",
    num_templates: "5",
    queries_per_template: "300",
    selectivity_column: defaultCol,
    selectivity_min: "0.10",
    selectivity_max: "0.20",
    max_predicates: "4",
    max_payload_columns: "4",
    dist_column: defaultCol,
    dist_name: "uniform",
    dist_min: "0",
    dist_max: "100",
    dist_mean: "50",
    dist_std: "10",
    dist_a: "2",
    temporal_enabled: false,
    temporal_pattern: "uniform",
    temporal_start: "2025-07-01T00:00:00",
    temporal_qpm: "300",
  };
}

function buildDriftTypeOptions(selected, options) {
  return options
    .map((opt) => `<option value="${opt.value}" ${opt.value === selected ? "selected" : ""}>${opt.label}</option>`)
    .join("");
}

function renderTemporalFields(item, kind) {
  return `
    <label class="field compact">
      <span>${t("temporal_drift_label")}</span>
      <label class="toggle-row">
        <input type="checkbox" data-kind="${kind}" data-action="temporal_enabled" ${item.temporal_enabled ? "checked" : ""} />
        <span>${t("temporal_generate_pattern")}</span>
      </label>
    </label>
    ${item.temporal_enabled ? `
      <div class="inline-row wrap">
        <label class="field compact flex-1">
          <span>Pattern</span>
          <select data-kind="${kind}" data-action="temporal_pattern">
            <option value="uniform" ${item.temporal_pattern === "uniform" ? "selected" : ""}>uniform</option>
            <option value="periodic" ${item.temporal_pattern === "periodic" ? "selected" : ""}>periodic</option>
            <option value="trend" ${item.temporal_pattern === "trend" ? "selected" : ""}>trend</option>
            <option value="long_tail" ${item.temporal_pattern === "long_tail" ? "selected" : ""}>long_tail</option>
          </select>
        </label>
        <label class="field compact flex-1">
          <span>Start Time</span>
          <input data-kind="${kind}" data-action="temporal_start" value="${escAttr(item.temporal_start)}" />
        </label>
        <label class="field compact flex-1">
          <span>QPM</span>
          <input data-kind="${kind}" data-action="temporal_qpm" value="${escAttr(item.temporal_qpm)}" />
        </label>
      </div>
      ${kind === "data" ? `
        <label class="field compact">
          <span>${t("timestamp_column_label")}</span>
          <input data-kind="${kind}" data-action="temporal_column" value="${escAttr(item.temporal_column)}" />
        </label>
      ` : ""}
    ` : ""}
  `;
}

function renderDataDriftItems() {
  if (dataDriftItems.length === 0) {
    dataDriftList.innerHTML = `<div class="empty">${t("no_data_drift")}</div>`;
    return;
  }
  dataDriftList.innerHTML = dataDriftItems
    .map((item, idx) => {
      const typeField = (() => {
        if (item.drift_type === "value_skew") {
          return `
            <label class="field compact">
              <span>${t("columns_csv_label")}</span>
              <input data-kind="data" data-action="columns" value="${escAttr(item.columns)}" placeholder="age,hours_per_week" />
            </label>
            <div class="inline-row wrap">
              <label class="field compact flex-1">
                <span>portion</span>
                <input data-kind="data" data-action="portion" value="${escAttr(item.portion)}" />
              </label>
              <label class="field compact flex-1">
                <span>skewness</span>
                <input data-kind="data" data-action="skewness" value="${escAttr(item.skewness)}" />
              </label>
            </div>
          `;
        }
        if (item.drift_type === "vary_cardinality") {
          return `
            <label class="field compact">
              <span>scale</span>
              <input data-kind="data" data-action="scale" value="${escAttr(item.scale)}" />
            </label>
          `;
        }
        if (item.drift_type === "selective_deletion" || item.drift_type === "insert_records") {
          return `
            <label class="field compact">
              <span>n</span>
              <input data-kind="data" data-action="n" value="${escAttr(item.n)}" />
            </label>
          `;
        }
        if (item.drift_type === "outlier_injection") {
          return `
            <div class="inline-row wrap">
              <label class="field compact flex-1">
                <span>column</span>
                <input data-kind="data" data-action="outlier_column" value="${escAttr(item.outlier_column)}" />
              </label>
              <label class="field compact flex-1">
                <span>n</span>
                <input data-kind="data" data-action="n" value="${escAttr(item.n)}" />
              </label>
            </div>
          `;
        }
        return `
          <div class="inline-row wrap">
            <label class="field compact flex-1">
              <span>timestamp_column</span>
              <input data-kind="data" data-action="timestamp_column" value="${escAttr(item.timestamp_column)}" />
            </label>
            <label class="field compact flex-1">
              <span>pattern</span>
              <select data-kind="data" data-action="temporal_pattern">
                <option value="uniform" ${item.temporal_pattern === "uniform" ? "selected" : ""}>uniform</option>
                <option value="periodic" ${item.temporal_pattern === "periodic" ? "selected" : ""}>periodic</option>
                <option value="trend" ${item.temporal_pattern === "trend" ? "selected" : ""}>trend</option>
                <option value="long_tail" ${item.temporal_pattern === "long_tail" ? "selected" : ""}>long_tail</option>
              </select>
            </label>
          </div>
          <div class="inline-row wrap">
            <label class="field compact flex-1">
              <span>start_time</span>
              <input data-kind="data" data-action="temporal_start" value="${escAttr(item.temporal_start)}" />
            </label>
            <label class="field compact flex-1">
              <span>queries_per_minute</span>
              <input data-kind="data" data-action="temporal_qpm" value="${escAttr(item.temporal_qpm)}" />
            </label>
          </div>
        `;
      })();
      return `
        <div class="drift-item-card" data-item-kind="data" data-id="${item.id}">
          <div class="drift-item-head">
            <div class="drift-item-title">${t("data_item_title", { index: idx + 1 })}</div>
            <button class="ghost mini-btn" type="button" data-action="remove">${t("remove_btn")}</button>
          </div>
          <label class="field compact">
            <span>${t("name_optional")}</span>
            <input data-kind="data" data-action="name" value="${escAttr(item.name)}" placeholder="value_skew_age" />
          </label>
          <label class="field compact">
            <span>${t("drift_type_label")}</span>
            <select data-kind="data" data-action="drift_type">
              ${buildDriftTypeOptions(item.drift_type, [
                { value: "value_skew", label: "value_skew" },
                { value: "vary_cardinality", label: "vary_cardinality" },
                { value: "selective_deletion", label: "selective_deletion" },
                { value: "outlier_injection", label: "outlier_injection" },
                { value: "insert_records", label: "insert_records" },
                { value: "add_timestamp", label: "add_timestamp" },
              ])}
            </select>
          </label>
          ${typeField}
          ${item.drift_type === "add_timestamp" ? `<p class="hint">${t("data_drift_temporal_hint")}</p>` : renderTemporalFields(item, "data")}
        </div>
      `;
    })
    .join("");
}

function renderWorkloadDriftItems() {
  if (workloadDriftItems.length === 0) {
    workloadDriftList.innerHTML = `<div class="empty">${t("no_workload_drift")}</div>`;
    return;
  }
  workloadDriftList.innerHTML = workloadDriftItems
    .map((item, idx) => {
      const typeField = (() => {
        if (item.drift_type === "selectivity") {
          return `
            <div class="inline-row wrap">
              <label class="field compact flex-1">
                <span>column</span>
                <input data-kind="workload" data-action="selectivity_column" value="${escAttr(item.selectivity_column)}" />
              </label>
              <label class="field compact flex-1">
                <span>min</span>
                <input data-kind="workload" data-action="selectivity_min" value="${escAttr(item.selectivity_min)}" />
              </label>
              <label class="field compact flex-1">
                <span>max</span>
                <input data-kind="workload" data-action="selectivity_max" value="${escAttr(item.selectivity_max)}" />
              </label>
            </div>
          `;
        }
        if (item.drift_type === "structure") {
          return `
            <div class="inline-row wrap">
              <label class="field compact flex-1">
                <span>max_predicates</span>
                <input data-kind="workload" data-action="max_predicates" value="${escAttr(item.max_predicates)}" />
              </label>
              <label class="field compact flex-1">
                <span>max_payload_columns</span>
                <input data-kind="workload" data-action="max_payload_columns" value="${escAttr(item.max_payload_columns)}" />
              </label>
            </div>
          `;
        }
        if (item.drift_type === "distribution") {
          return `
            <div class="inline-row wrap">
              <label class="field compact flex-1">
                <span>column</span>
                <input data-kind="workload" data-action="dist_column" value="${escAttr(item.dist_column)}" />
              </label>
              <label class="field compact flex-1">
                <span>distribution</span>
                <select data-kind="workload" data-action="dist_name">
                  <option value="uniform" ${item.dist_name === "uniform" ? "selected" : ""}>uniform</option>
                  <option value="normal" ${item.dist_name === "normal" ? "selected" : ""}>normal</option>
                  <option value="zipf" ${item.dist_name === "zipf" ? "selected" : ""}>zipf</option>
                </select>
              </label>
            </div>
            ${item.dist_name === "normal" ? `
              <div class="inline-row wrap">
                <label class="field compact flex-1">
                  <span>mean</span>
                  <input data-kind="workload" data-action="dist_mean" value="${escAttr(item.dist_mean)}" />
                </label>
                <label class="field compact flex-1">
                  <span>std</span>
                  <input data-kind="workload" data-action="dist_std" value="${escAttr(item.dist_std)}" />
                </label>
              </div>
            ` : `
              <div class="inline-row wrap">
                <label class="field compact flex-1">
                  <span>min</span>
                  <input data-kind="workload" data-action="dist_min" value="${escAttr(item.dist_min)}" />
                </label>
                <label class="field compact flex-1">
                  <span>max</span>
                  <input data-kind="workload" data-action="dist_max" value="${escAttr(item.dist_max)}" />
                </label>
                ${item.dist_name === "zipf" ? `
                  <label class="field compact flex-1">
                    <span>a</span>
                    <input data-kind="workload" data-action="dist_a" value="${escAttr(item.dist_a)}" />
                  </label>
                ` : ""}
              </div>
            `}
          `;
        }
        return `<p class="hint">${t("baseline_hint")}</p>`;
      })();
      return `
        <div class="drift-item-card" data-item-kind="workload" data-id="${item.id}">
          <div class="drift-item-head">
            <div class="drift-item-title">${t("workload_item_title", { index: idx + 1 })}</div>
            <button class="ghost mini-btn" type="button" data-action="remove">${t("remove_btn")}</button>
          </div>
          <label class="field compact">
            <span>${t("name_optional")}</span>
            <input data-kind="workload" data-action="name" value="${escAttr(item.name)}" placeholder="selectivity_shift_1" />
          </label>
          <label class="field compact">
            <span>${t("drift_type_label")}</span>
            <select data-kind="workload" data-action="drift_type">
              ${buildDriftTypeOptions(item.drift_type, [
                { value: "baseline", label: "baseline" },
                { value: "selectivity", label: "selectivity" },
                { value: "structure", label: "structure" },
                { value: "distribution", label: "distribution" },
              ])}
            </select>
          </label>
          <div class="inline-row wrap">
            <label class="field compact flex-1">
              <span>num_templates</span>
              <input data-kind="workload" data-action="num_templates" value="${escAttr(item.num_templates)}" />
            </label>
            <label class="field compact flex-1">
              <span>queries_per_template</span>
              <input data-kind="workload" data-action="queries_per_template" value="${escAttr(item.queries_per_template)}" />
            </label>
          </div>
          ${typeField}
          ${renderTemporalFields(item, "workload")}
        </div>
      `;
    })
    .join("");
}

function renderDriftPaneState() {
  const dataOn = driftDataEnabled.checked;
  const workloadOn = driftWorkloadEnabled.checked;
  dataDriftPane.classList.toggle("pane-disabled", !dataOn);
  workloadDriftPane.classList.toggle("pane-disabled", !workloadOn);
  addDataDriftBtn.disabled = !dataOn;
  addWorkloadDriftBtn.disabled = !workloadOn;
}

function updateDriftItem(kind, id, action, value, inputType) {
  const list = kind === "data" ? dataDriftItems : workloadDriftItems;
  const item = list.find((x) => x.id === id);
  if (!item) return;
  const nextValue = inputType === "checkbox" ? !!value : String(value ?? "");
  item[action] = nextValue;
  const mustRerender =
    action === "drift_type"
    || action === "temporal_enabled"
    || action === "dist_name";
  if (mustRerender) {
    if (kind === "data") renderDataDriftItems();
    else renderWorkloadDriftItems();
  }
}

function attachDriftListHandlers(listEl, kind) {
  listEl.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-action='remove']");
    if (!btn) return;
    const card = event.target.closest("[data-id]");
    const id = card?.getAttribute("data-id");
    if (!id) return;
    if (kind === "data") {
      dataDriftItems = dataDriftItems.filter((x) => x.id !== id);
      renderDataDriftItems();
      return;
    }
    workloadDriftItems = workloadDriftItems.filter((x) => x.id !== id);
    renderWorkloadDriftItems();
  });

  const onField = (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const action = target.getAttribute("data-action");
    if (!action || action === "remove") return;
    const input = target;
    const card = input.closest("[data-id]");
    const id = card?.getAttribute("data-id");
    if (!id) return;
    if (input instanceof HTMLInputElement && input.type === "checkbox") {
      updateDriftItem(kind, id, action, input.checked, "checkbox");
      return;
    }
    if (input instanceof HTMLInputElement || input instanceof HTMLSelectElement) {
      updateDriftItem(kind, id, action, input.value, input.type);
    }
  };
  listEl.addEventListener("change", onField);
  listEl.addEventListener("input", onField);
}

function withYamlSuffix(basePath, suffix) {
  const clean = (basePath || "").trim() || "driftspec/generated/service_spec.yaml";
  if (/\.(yaml|yml)$/i.test(clean)) {
    return clean.replace(/\.(yaml|yml)$/i, `_${suffix}.yaml`);
  }
  return `${clean}_${suffix}.yaml`;
}

function buildDataSpecObject() {
  const sourceType = schemaSourceType.value;
  if (sourceType !== "csv") {
    throw new Error(t("data_drift_csv_required"));
  }
  const csvPath = schemaDataPath.value.trim();
  if (!csvPath) {
    throw new Error(t("data_drift_csv_path_required"));
  }
  if (dataDriftItems.length === 0) {
    throw new Error(t("add_one_data_drift"));
  }
  const baseTable = defaultBaseTable();
  const schemaPath = getSchemaPathForSpec();
  const patternId = `service_data_${Date.now()}`;
  const defaultCols = Object.keys(currentSchemaNorm?.tables?.[baseTable]?.columns || {});
  const drifts = [];

  dataDriftItems.forEach((item, idx) => {
    const driftName = sanitizeName(item.name || `${item.drift_type}_${idx + 1}`);
    const outPath = `output/data/service_generated/${patternId}_${driftName}.csv`;
    const drift = {
      name: driftName,
      drift_type: item.drift_type,
      output_path: outPath,
    };
    if (item.drift_type === "value_skew") {
      const cols = splitColumns(item.columns);
      drift.columns = cols.length ? cols : (defaultCols.length ? [defaultCols[0]] : []);
      drift.portion = parseNum(item.portion, 1.0);
      drift.skewness = parseNum(item.skewness, 2);
    } else if (item.drift_type === "vary_cardinality") {
      drift.scale = parseNum(item.scale, 1.2);
    } else if (item.drift_type === "selective_deletion" || item.drift_type === "insert_records") {
      drift.n = parseIntNum(item.n, 1000);
    } else if (item.drift_type === "outlier_injection") {
      drift.column = item.outlier_column || defaultCols[0];
      drift.n = parseIntNum(item.n, 1000);
      if (!drift.column) {
        throw new Error(t("outlier_column_required"));
      }
    } else if (item.drift_type === "add_timestamp") {
      drift.timestamp_column = item.timestamp_column || "timestamp";
      drift.start_time = item.temporal_start || "2025-07-01T00:00:00";
      drift.pattern = item.temporal_pattern || "uniform";
      drift.queries_per_minute = parseIntNum(item.temporal_qpm, 300);
    }
    drifts.push(drift);

    if (item.temporal_enabled && item.drift_type !== "add_timestamp") {
      drifts.push({
        name: `${driftName}_temporal`,
        drift_type: "add_timestamp",
        source_path: outPath,
        timestamp_column: item.temporal_column || "timestamp",
        start_time: item.temporal_start || "2025-07-01T00:00:00",
        pattern: item.temporal_pattern || "uniform",
        queries_per_minute: parseIntNum(item.temporal_qpm, 300),
        output_path: outPath.replace(/\.csv$/i, "_temporal.csv"),
      });
    }
  });

  return {
    pattern_id: patternId,
    seed: 42,
    type: { family: "data", category: "drift", subtype: "single_table" },
    data_source: {
      kind: "csv",
      path: csvPath,
      schema_extractor: {
        source_type: "csv",
        sample_size: parseIntNum(schemaSampleSize.value, 1000),
        schema_output_path: schemaPath,
      },
    },
    variables: {
      base_table: baseTable,
      drifts,
    },
  };
}

function buildWorkloadSpecObject() {
  if (workloadDriftItems.length === 0) {
    throw new Error(t("add_one_workload_drift"));
  }
  const sourceType = schemaSourceType.value;
  const baseTable = defaultBaseTable();
  const schemaPath = getSchemaPathForSpec();
  const patternId = `service_workload_${Date.now()}`;
  const sampleSize = parseIntNum(schemaSampleSize.value, 1000);
  const dataSource = {};

  if (sourceType === "csv") {
    const csvPath = schemaDataPath.value.trim();
    if (!csvPath) {
      throw new Error(t("workload_csv_path_required"));
    }
    dataSource.kind = "csv";
    dataSource.path = csvPath;
    dataSource.schema_extractor = {
      source_type: "csv",
      sample_size: sampleSize,
      schema_output_path: schemaPath,
    };
  } else if (sourceType === "postgres") {
    const cfg = schemaDbConfigPath.value.trim();
    if (!cfg) {
      throw new Error(t("workload_postgres_config_required"));
    }
    dataSource.kind = "postgres";
    dataSource.db_config_path = cfg;
    dataSource.schema_name = (schemaName.value || "public").trim() || "public";
    dataSource.physical_table = baseTable.split(".").pop();
    dataSource.schema_extractor = {
      source_type: "postgres",
      sample_size: sampleSize,
      schema_output_path: schemaPath,
    };
  } else {
    throw new Error(t("unsupported_source_type", { sourceType }));
  }

  const runs = [];
  const queryRuns = [];
  workloadDriftItems.forEach((item, idx) => {
    const nameStem = sanitizeName(item.name || `${item.drift_type}_${idx + 1}`);
    const runName = `run_${nameStem}`;
    const run = {
      name: runName,
      output_path: `output/intermediate_yaml/service_generated/${patternId}_${runName}_templates.json`,
      num_templates: parseIntNum(item.num_templates, 5),
    };

    if (item.drift_type === "selectivity") {
      const col = item.selectivity_column || firstColumnForTable(baseTable);
      if (!col) {
        throw new Error(t("selectivity_column_required"));
      }
      run.selectivity = {
        [col]: [parseNum(item.selectivity_min, 0.1), parseNum(item.selectivity_max, 0.2)],
      };
    } else if (item.drift_type === "structure") {
      run.max_predicates = parseIntNum(item.max_predicates, 4);
      run.max_payload_columns = parseIntNum(item.max_payload_columns, 4);
    }
    runs.push(run);

    const qrun = {
      name: `queries_${nameStem}`,
      template: runName,
      queries_per_template: parseIntNum(item.queries_per_template, 300),
      outputs: [
        {
          type: "workload",
          path: `output/workload/service_generated/${patternId}_${runName}.csv`,
        },
      ],
    };

    if (item.drift_type === "distribution") {
      const col = item.dist_column || firstColumnForTable(baseTable);
      if (!col) {
        throw new Error(t("distribution_column_required"));
      }
      const key = `${baseTable}.${col}`;
      const distName = item.dist_name || "uniform";
      const dist = { distribution: distName };
      if (distName === "normal") {
        dist.mean = parseNum(item.dist_mean, 50);
        dist.std = parseNum(item.dist_std, 10);
      } else {
        dist.min = parseNum(item.dist_min, 0);
        dist.max = parseNum(item.dist_max, 100);
        if (distName === "zipf") {
          dist.a = parseNum(item.dist_a, 2);
        }
      }
      qrun.dist_config = { [key]: dist };
    }

    if (item.temporal_enabled) {
      qrun.outputs.push({
        type: "temporal",
        path: `output/temporal/service_generated/${patternId}_${runName}_${item.temporal_pattern || "uniform"}.csv`,
        timestamp: {
          pattern: item.temporal_pattern || "uniform",
          start_time: item.temporal_start || "2025-07-01T00:00:00",
          queries_per_minute: parseIntNum(item.temporal_qpm, 300),
        },
      });
    }
    queryRuns.push(qrun);
  });

  return {
    pattern_id: patternId,
    seed: 42,
    type: { family: "workload", category: "templates", subtype: "selection_payload" },
    data_source: dataSource,
    variables: {
      base_table: baseTable,
      schema_path: schemaPath,
      defaults: {
        num_templates: 5,
        max_predicates: 4,
        max_payload_columns: 4,
      },
      runs,
      query_runs: queryRuns,
    },
  };
}

async function buildDriftSpecs() {
  const buildTargets = [];
  if (driftDataEnabled.checked) {
    buildTargets.push({ kind: "data", spec: buildDataSpecObject() });
  }
  if (driftWorkloadEnabled.checked) {
    buildTargets.push({ kind: "workload", spec: buildWorkloadSpecObject() });
  }
  if (buildTargets.length === 0) {
    alert(t("enable_one_drift_card"));
    return;
  }

  const basePath = generatedSpecPath.value.trim() || "driftspec/generated/service_spec.yaml";
  buildSpecStatus.textContent = t("building_driftspec");
  const outputPaths = [];

  for (const item of buildTargets) {
    const outPath = buildTargets.length > 1 ? withYamlSuffix(basePath, item.kind) : basePath;
    const res = await apiPost("/api/spec/build", {
      spec: item.spec,
      output_path: outPath,
    });
    outputPaths.push(res.path);
  }

  buildSpecStatus.textContent = t("generated_paths", { paths: outputPaths.join(" | ") });
  specPath.value = outputPaths[0] || "";
  await loadSpecs();
}

function initDriftBuilder() {
  if (!generatedSpecPath.value.trim()) {
    generatedSpecPath.value = "driftspec/generated/service_spec.yaml";
  }
  if (dataDriftItems.length === 0) {
    dataDriftItems.push(makeDataDriftItem());
  }
  renderDataDriftItems();
  renderWorkloadDriftItems();
  renderDriftPaneState();
}

async function extractSchema() {
  const sourceType = schemaSourceType.value;
  const payload = {
    source_type: sourceType,
  };

  if (sourceType === "postgres") {
    let cfg = schemaDbConfigPath.value.trim();
    let name = schemaName.value.trim();
    if (!cfg) {
      const content = schemaDbConfigText.value.trim();
      if (!content) {
        alert(t("upload_or_paste_db_config"));
        return;
      }
      try {
        const filename = `db_config_${Date.now()}.json`;
        cfg = await saveTextFile(content, filename);
        schemaDbConfigPath.value = cfg;
        schemaDbSaveStatus.textContent = t("saved_path", { path: cfg });
      } catch (err) {
        alert(t("save_db_config_failed", { error: err.message }));
        return;
      }
    }
    if (!name) {
      name = "public";
      schemaName.value = name;
    }
    payload.db_config_path = cfg;
    payload.schema_name = name;
  } else {
    const path = schemaDataPath.value.trim();
    if (!path) {
      alert(t("enter_data_path"));
      return;
    }
    payload.path = path;
  }

  const sample = schemaSampleSize.value.trim();
  if (sample) payload.sample_size = sample;
  const out = schemaOutputPath.value.trim();
  if (out) payload.output_path = out;

  try {
    const data = await apiPost("/api/schema/extract", payload);
    await refreshJobs();
    schemaLiveStatus.textContent = translateJobStatus(data.job.status || "queued");
    schemaLiveLog.textContent = t("schema_job_submitted");
    renderLogs(data.job);
    startPolling(data.job.id);
  } catch (err) {
    alert(t("generate_failed", { error: err.message }));
  }
}

async function uploadFile(file) {
  const dataUrl = await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error(t("read_file_failed")));
    reader.readAsDataURL(file);
  });
  const base64 = String(dataUrl).split(",")[1] || "";
  const resp = await apiPost("/api/files/upload", {
    filename: file.name,
    content_b64: base64,
  });
  return resp.path;
}

async function saveTextFile(content, filename) {
  const resp = await apiPost("/api/files/save-text", {
    filename,
    content,
  });
  return resp.path;
}

async function loadDbConfigs() {
  try {
    const data = await apiGet("/api/uploads?ext=.json&prefix=db_config_");
    schemaDbConfigSelect.innerHTML = "";
    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = t("select_config");
    schemaDbConfigSelect.appendChild(empty);
    (data.files || []).forEach((file) => {
      const opt = document.createElement("option");
      opt.value = file;
      opt.textContent = file.split("/").pop();
      schemaDbConfigSelect.appendChild(opt);
    });
  } catch (err) {
    schemaDbConfigSelect.innerHTML = `<option value="">${t("load_failed")}</option>`;
  }
}

schemaDataFile.addEventListener("change", async (event) => {
  const file = event.target.files?.[0];
  if (!file) return;
  schemaDataUploadStatus.textContent = t("uploading");
  try {
    const path = await uploadFile(file);
    schemaDataPath.value = path;
    schemaDataUploadStatus.textContent = t("saved_path", { path });
    maybeSetSchemaOutputPath();
  } catch (err) {
    schemaDataUploadStatus.textContent = t("failed_with_error", { error: err.message });
  }
});

schemaDbConfigFile.addEventListener("change", async (event) => {
  const file = event.target.files?.[0];
  if (!file) return;
  schemaDbUploadStatus.textContent = t("uploading");
  try {
    const path = await uploadFile(file);
    schemaDbConfigPath.value = path;
    schemaDbUploadStatus.textContent = t("saved_path", { path });
    loadDbConfigs();
    maybeSetSchemaOutputPath();
  } catch (err) {
    schemaDbUploadStatus.textContent = t("failed_with_error", { error: err.message });
  }
});

saveCsvBtn.addEventListener("click", async () => {
  const content = schemaCsvText.value.trim();
  if (!content) {
    alert(t("enter_csv_content"));
    return;
  }
  schemaCsvSaveStatus.textContent = t("saving");
  try {
    const filename = `manual_${Date.now()}.csv`;
    const path = await saveTextFile(content, filename);
    schemaDataPath.value = path;
    schemaCsvSaveStatus.textContent = t("saved_path", { path });
    maybeSetSchemaOutputPath();
  } catch (err) {
    schemaCsvSaveStatus.textContent = t("failed_with_error", { error: err.message });
  }
});

saveDbConfigBtn.addEventListener("click", async () => {
  const content = schemaDbConfigText.value.trim();
  if (!content) {
    alert(t("enter_db_json"));
    return;
  }
  schemaDbSaveStatus.textContent = t("saving");
  try {
    const filename = `db_config_${Date.now()}.json`;
    const path = await saveTextFile(content, filename);
    schemaDbConfigPath.value = path;
    schemaDbSaveStatus.textContent = t("saved_path", { path });
    loadDbConfigs();
    maybeSetSchemaOutputPath();
  } catch (err) {
    schemaDbSaveStatus.textContent = t("failed_with_error", { error: err.message });
  }
});

schemaDbConfigSelect.addEventListener("change", () => {
  if (schemaDbConfigSelect.value) {
    schemaDbConfigPath.value = schemaDbConfigSelect.value;
    maybeSetSchemaOutputPath();
  }
});

refreshDbConfigs.addEventListener("click", loadDbConfigs);
refreshSchemaFiles.addEventListener("click", loadSchemaFiles);
loadSchemaBtn.addEventListener("click", () => {
  const path = schemaFileSelect.value;
  if (!path) {
    alert(t("select_schema_first"));
    return;
  }
  loadSchemaPreview(path);
});
visualizeSchemaBtn.addEventListener("click", async () => {
  await loadSchemaFiles();
  const path = schemaFileSelect.value;
  if (!path) {
    alert(t("no_visualizable_schema"));
    return;
  }
  loadSchemaPreview(path);
});
schemaFileSelect.addEventListener("change", () => {
  if (schemaFileSelect.value) {
    loadSchemaPreview(schemaFileSelect.value);
  }
});
schemaTableList.addEventListener("click", (event) => {
  const chip = event.target.closest("[data-table-name]");
  if (!chip || !currentSchemaNorm) return;
  const tableName = chip.getAttribute("data-table-name");
  if (!tableName || !currentSchemaNorm.tables[tableName]) return;
  toggleSelectedTable(tableName);
});
schemaPreview.addEventListener("click", (event) => {
  const btn = event.target.closest("[data-action='toggle-table']");
  if (!btn) return;
  const tableName = btn.getAttribute("data-table-name");
  if (!tableName) return;
  tableDetailCollapsed[tableName] = !tableDetailCollapsed[tableName];
  renderSelectedTablesDetails();
});
schemaDataPath.addEventListener("input", () => maybeSetSchemaOutputPath());
schemaName.addEventListener("input", () => maybeSetSchemaOutputPath());

specSelect.addEventListener("change", (event) => {
  if (event.target.value) {
    specPath.value = event.target.value;
  }
});

if (languageSelect) {
  languageSelect.addEventListener("change", (event) => {
    setLanguage(event.target.value);
  });
}

runBtn.addEventListener("click", runSpec);
traceBtn.addEventListener("click", runTrace);
schemaSourceType.addEventListener("change", updateSchemaFields);
schemaBtn.addEventListener("click", extractSchema);
addDataDriftBtn.addEventListener("click", () => {
  dataDriftItems.push(makeDataDriftItem());
  renderDataDriftItems();
});
addWorkloadDriftBtn.addEventListener("click", () => {
  workloadDriftItems.push(makeWorkloadDriftItem());
  renderWorkloadDriftItems();
});
driftDataEnabled.addEventListener("change", renderDriftPaneState);
driftWorkloadEnabled.addEventListener("change", renderDriftPaneState);
buildSpecBtn.addEventListener("click", async () => {
  try {
    await buildDriftSpecs();
  } catch (err) {
    buildSpecStatus.textContent = t("build_failed", { error: err.message });
    alert(t("build_failed", { error: err.message }));
  }
});
attachDriftListHandlers(dataDriftList, "data");
attachDriftListHandlers(workloadDriftList, "workload");

currentLang = detectInitialLanguage();
applyStaticTranslations();
updateSchemaFields();
maybeSetSchemaOutputPath(true);
initDriftBuilder();
initSideNavToggle();
initSideNav();
renderDefaultPanels();
checkHealth();
loadSpecs();
loadSchemaFiles();
refreshJobs();
setInterval(refreshJobs, 5000);
