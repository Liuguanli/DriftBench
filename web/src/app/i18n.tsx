import * as React from "react";

type Language = "en" | "zh";

const messages = {
  en: {
    common: {
      language: "Language",
      english: "English",
      chinese: "中文",
      previous: "Previous",
      next: "Next",
      back: "Back",
    },
    nav: {
      home: "Home",
      driftTypes: "Drift Types",
      generator: "Generator",
      visualization: "Visualization",
      caseStudies: "Case Studies",
      subtitle: "Drift-Aware Database Benchmarking",
      footerLine1:
        "Based on the research paper by Guanli Liu and Renata Borovica-Gajic, The University of Melbourne",
      footerLink: "View on GitHub",
    },
    home: {
      tagline: "Drift-Aware Database Benchmarking",
      title: "DriftBench",
      subtitle:
        "A unified foundation for drift-aware benchmarking, enabling evaluation of database systems under evolving data and workloads through DriftSpec specifications.",
      explore: "Explore Drift Types",
      generate: "Generate Drift",
      features: {
        taxonomyTitle: "Taxonomy of Drift",
        taxonomyDesc:
          "Conceptualize data, workload, and temporal drift within a unified taxonomy",
        taxonomyItems: [
          "Cardinality drift (scaling, updating)",
          "Distribution drift (column shifts, outliers)",
          "Workload drift (predicates, selectivity, structure)",
          "Temporal patterns (uniform, periodic, trend)",
        ],
        generatorTitle: "Drift Generator",
        generatorDesc:
          "Interactive tool to upload datasets and generate custom drift scenarios",
        generatorItems: [
          "Upload CSV or connect to database",
          "Configure data and workload drift",
          "Automatic DriftSpec generation",
          "Download ready-to-use YAML files",
        ],
        frameworkTitle: "Prototype Framework",
        frameworkDesc:
          "Modular framework implementing DriftSpec for synthesis and evaluation",
        frameworkItems: [
          "Schema extraction and analysis",
          "Distribution simulation",
          "Data and workload generation",
          "Timestamp generation for temporal drift",
        ],
      },
      contributionsTitle: "Key Contributions",
      contributions: [
        {
          title: "Making Drift a First-Class Concept",
          desc:
            "Shift benchmarking from static, one-off tests to controlled, continuous evaluation under drift",
        },
        {
          title: "Standardized Executable Language",
          desc:
            "Study how data and workload evolution influence database behavior through reproducible specifications",
        },
        {
          title: "Benchmark Integration",
          desc:
            "Compatible with TPC-H, RedBench, and custom schemas for both benchmark-driven and trace-driven settings",
        },
        {
          title: "Practical Evaluation",
          desc:
            "Demonstrated through case studies on cardinality estimators and learned indices under drift",
        },
      ],
      ctaTitle: "Ready to Get Started?",
      ctaDesc:
        "Explore different drift types, generate your own DriftSpec configurations, and visualize how drift affects database components",
      ctaPrimary: "Learn About Drift Types",
      ctaSecondary: "See Visualizations",
    },
    driftTypes: {
      title: "Drift Taxonomy",
      subtitle:
        "A working taxonomy of data and workload drift that captures recurring patterns observed across prior systems and benchmarks.",
      tabs: {
        data: "Data Drift",
        workload: "Workload Drift",
        temporal: "Temporal Drift",
      },
      data: {
        introTitle: "What is Data Drift?",
        introDesc:
          "Data drift refers to changes in the cardinality or distribution of records within a database.",
        definition:
          "Definition: Let D₁ and D₂ denote two versions of a dataset over the same schema S. Data drift is a significant change in the statistical properties or volume of data between D₁ and D₂.",
        scalingTitle: "Scaling Cardinality",
        scalingDesc: "Changes in the overall number of records",
        cardinalityBadge: "Cardinality Drift",
        scalingBody:
          "Models net size effects under a new snapshot (e.g., 0.5×, 2×, 10×) without prescribing how tuples arrived or departed.",
        exampleLabel: "Example",
        scalingExample:
          "Population records gradually increase as coverage expands to new regions or higher birth rates",
        useCases: "Use Cases",
        scalingUses: [
          "Stress storage footprint",
          "Evaluate plan costs under larger snapshots",
          "Test system scalability (TPC-H, TPC-DS)",
        ],
        updatingTitle: "Updating Cardinality",
        updatingDesc: "Time-ordered stream of inserts and deletes",
        updatingBody:
          "Dataset is mutated over time rather than resized in one shot. Applies continuous insertions and deletions.",
        updatingExample:
          "Individuals continuously added and removed due to births, deaths, and migration",
        updatingUses: [
          "Index maintenance evaluation",
          "Statistics freshness testing",
          "Continuous-update behavior analysis",
        ],
        shiftingTitle: "Shifting Column Distributions",
        shiftingDesc: "Changes in column value distributions",
        distributionBadge: "Distributional Drift",
        shiftingBody:
          "Captures changes such as increased skewness without altering dataset cardinality. Affects spatial and temporal workloads.",
        shiftingExample:
          "Student concentration during enrollment period shifts distribution of education-related attributes",
        shiftingUses: [
          "Spatial workload evaluation",
          "Selectivity estimation testing",
          "Distribution-aware index performance",
        ],
        outliersTitle: "Injecting Outliers",
        outliersDesc: "Rare or extreme values for robustness testing",
        outliersBody:
          "Uses rare or extreme values to test system robustness under distributional anomalies and potential data poisoning.",
        outliersExample:
          "Small number of records with extremely large household sizes or unusually high incomes",
        outliersUses: [
          "Optimizer robustness testing",
          "Learned index data poisoning studies",
          "Statistics distortion evaluation",
        ],
      },
      workload: {
        introTitle: "What is Workload Drift?",
        introDesc:
          "Workload drift refers to changes in the structure or statistical properties of queries executed against a database over time.",
        definition:
          "Definition: A workload W is defined as a distribution P(Wτ(θ)) over queries instantiated from a parameterized template τ, where θ ∈ Θ denotes the parameter-generating operator.",
        predicateTitle: "Changing Predicate Distributions",
        predicateDesc: "Statistical distribution of predicates over time",
        parametricBadge: "Parametric Drift",
        predicateBody:
          "Drifts in the statistical distribution of predicates. Affects query optimizers and learned indices that rely on historical access patterns.",
        predicateExample:
          "Census queries increasingly concentrate on major cities rather than evenly across all regions",
        impactLabel: "Impact",
        predicateImpact: [
          "Query optimizer accuracy",
          "Learned index effectiveness",
          "Access pattern optimization",
        ],
        selectivityTitle: "Varying Selectivity",
        selectivityDesc: "Queries from same template with varying ranges",
        selectivityBody:
          "Queries from the same logical template exhibit varying predicate ranges, leading to different join strategies or plan choices.",
        selectivityExample:
          "Census workload repeatedly uses same query template while expanding predicate (age) ranges",
        selectivityImpact: [
          "Plan stability",
          "Join strategy selection",
          "Cardinality estimation accuracy",
        ],
        structureTitle: "Modifying Query Structure",
        structureDesc: "Changes in query templates and conditions",
        structuralBadge: "Structural Drift",
        structureBody:
          "Changes in query templates, such as modified predicates or join conditions. Can trigger re-optimization or impact index usage.",
        structureExample:
          "Census queries extended with additional joins or predicates (household, employment tables)",
        structureImpact: [
          "Re-optimization requirements",
          "Index selection changes",
          "Query plan structure",
        ],
        payloadTitle: "Changing Payloads",
        payloadDesc: "Changes in projected column sets",
        payloadBody:
          "Changes in the set of projected columns. Distinct impact on I/O cost and column scan behavior compared to other structural changes.",
        payloadExample:
          "Workload evolves from lightweight aggregates to detailed record inspection",
        payloadImpact: [
          "I/O cost variations",
          "Column scan behavior",
          "Output schema definition",
        ],
      },
      temporal: {
        introTitle: "What is Temporal Drift?",
        introDesc:
          "Temporal drift models the evolution of data or workloads over time, following non-stationary patterns such as bursts, trends, or repeats.",
        introBody:
          "We adopt the classification from Sibyl, which defines four representative temporal patterns. These can be applied independently or in combination with data and workload drift.",
        uniformTitle: "Uniform Pattern",
        uniformDesc: "Constant rate over time",
        uniformBody:
          "Events or queries arrive at a constant rate with uniform distribution across the time window. Provides a baseline for comparison with other patterns.",
        periodicTitle: "Periodic Pattern",
        periodicDesc: "Repeating cycles",
        periodicBody:
          "Events follow repeating cycles with regular intervals. Common in real-world scenarios with daily, weekly, or seasonal patterns.",
        trendTitle: "Trend Pattern",
        trendDesc: "Gradual increase or decrease",
        trendBody:
          "Events show a gradual increasing or decreasing trend over time. Reflects long-term growth or decline in system usage.",
        tailTitle: "Long-tail Pattern",
        tailDesc: "Burst followed by decay",
        tailBody:
          "High initial activity followed by exponential decay. Common in event-driven scenarios and viral content access patterns.",
        combineTitle: "Combining Temporal with Data/Workload Drift",
        combineBody:
          "Temporal patterns can be applied independently or combined with data and workload drift to create more expressive scenarios:",
        combineItems: [
          "Query timestamps: Generated per instance to simulate realistic query streams with temporal variation",
          "Repetitive queries: Common in real-world cases, can be assigned periodic timestamps",
          "Data updates: Insertions and deletions can follow scheduled intervals to reproduce read-heavy or write-heavy workloads",
          "Cross-effects: Reveal performance degradation under bursty arrivals or delayed adaptation to trends",
        ],
      },
    },
    visualization: {
      title: "Drift Visualization",
      subtitle:
        "Interactive visualizations demonstrating different types of data and workload drift",
      tabs: {
        cardinality: "Data Cardinality",
        distribution: "Data Distribution",
        workload: "Workload",
        temporal: "Temporal",
      },
      cardinality: {
        title: "Cardinality Scaling Drift",
        desc: "Visualize how dataset size changes affect distribution",
        scaleLabel: "Cardinality Scale Factor",
        records: "records",
        original: "Original Dataset (D₁)",
        scaled: "Scaled Dataset (D₂)",
        impact:
          "Cardinality scaling affects storage footprint, query plan costs, and statistics accuracy. The distribution pattern remains similar, but the sheer volume can trigger different optimization strategies.",
      },
      distribution: {
        title: "Progressive Distribution Drift",
        desc: "Observe how data distribution evolves through overlapping shifts",
        stable: "Stable normal distribution",
        newMode: "New mode emerges (orange)",
        outliers: "Outliers injected (green)",
        columnTitle: "Column Distribution Shift",
        columnDesc: "Interactive skewness adjustment",
        skewness: "Skewness",
        balanced: "Balanced",
        moderate: "Moderate",
        heavy: "Heavy Skew",
        original: "Original Distribution",
        skewed: "Skewed Distribution",
        impact:
          "Distribution shifts can significantly affect cardinality estimation and query optimization. Increased skewness concentrates data in specific ranges, potentially making some predicates highly selective while others become ineffective.",
      },
      workload: {
        title: "Workload Drift Patterns",
        desc: "Changes in query predicate distributions over time",
        original: "Original Workload (W₁)",
        drifted: "Drifted Workload (W₂)",
        originalNote:
          "Uniform distribution of predicate values (age range centers)",
        driftedNote:
          "Predicate ranges shift and concentrate in higher values (parametric drift)",
        impact:
          "Workload drift affects learned query optimizers and cardinality estimators that rely on historical query patterns. Shifting predicate distributions can make cached statistics or learned models less accurate over time.",
      },
      temporal: {
        title: "Temporal Drift Patterns",
        desc: "How drift evolves over time with different patterns",
        patternLabel: "Temporal Pattern",
        uniform: "Uniform - Constant rate",
        periodic: "Periodic - Repeating cycles",
        trend: "Trend - Gradual increase",
        tail: "Long Tail - Burst with decay",
        axisTime: "Time (seconds)",
        axisRate: "Event Rate",
        uniformTitle: "Uniform Pattern",
        periodicTitle: "Periodic Pattern",
        trendTitle: "Trend Pattern",
        tailTitle: "Long Tail Pattern",
        uniformBody:
          "Events arrive at a constant rate. Provides baseline for comparison.",
        periodicBody:
          "Events follow repeating cycles with regular intervals. Common in daily/weekly patterns.",
        trendBody: "Events show gradual increase over time. Reflects long-term growth.",
        tailBody:
          "High initial activity followed by exponential decay. Common in viral events.",
        useCases: "Use Cases",
        useCasesUniform: [
          "Baseline performance testing",
          "Steady-state system behavior",
          "Controlled experiment setup",
        ],
        useCasesPeriodic: [
          "Business hours simulation",
          "Seasonal workload patterns",
          "Batch processing cycles",
        ],
        useCasesTrend: [
          "Growing user base simulation",
          "Long-term capacity planning",
          "Scalability testing",
        ],
        useCasesTail: [
          "Viral content access patterns",
          "Event-driven workloads",
          "Flash crowd scenarios",
        ],
      },
      labels: {
        dataPoints: "Data Points",
        datasetD1: "D₁",
        datasetD2New: "D₂ New",
        datasetD3Outliers: "D₃ Outliers",
        queries: "Queries",
        eventRate: "Event Rate",
        queryId: "Query ID",
        predicateCenter: "Predicate Center",
      },
    },
    caseStudies: {
      title: "Case Studies",
      subtitle:
        "Practical demonstrations of DriftBench applied to real-world scenarios using the Census dataset",
      researchTitle: "Research Context",
      researchBody:
        "These case studies are based on experiments from the paper \"Toward Drift-Aware Database Benchmarking\" by Guanli Liu and Renata Borovica-Gajic. The examples use the Census dataset to demonstrate how DriftBench enables controlled drift evaluation.",
      tabs: {
        data: "Data Drift",
        workload: "Workload Drift",
        estimator: "Estimator Evaluation",
      },
      data: {
        title: "Census Dataset: Data Drift Analysis",
        desc: "Analyzing cardinality updates and distribution shifts on Census data",
        overviewTitle: "Dataset Overview",
        overviewBody:
          "The Census Income dataset contains demographic information including age, workclass, education, marital status, occupation, and income. We select representative attributes (age - numeric, workclass - categorical) to demonstrate drift effects.",
        updatingTitle: "Updating Cardinality",
        updatingDesc: "10% random deletion maintaining distribution",
        cardinalityBadge: "Cardinality Drift",
        updatingObsTitle: "Observation:",
        updatingObsBody:
          "The deleted subset is sampled proportionally to maintain the original distribution shape, reflecting natural workload patterns without artificial bias.",
        shiftingTitle: "Shifting Column Distribution",
        shiftingDesc: "Increased skewness in categorical attributes",
        distBadge: "Distributional Drift",
        shiftingObsTitle: "Observation:",
        shiftingObsBody:
          "The most frequent category (Private) becomes more dominant, amplifying skewness and affecting selectivity estimation for predicates on this column.",
        keyFindings: "Key Findings",
        finding1Title: "Proportional Deletion",
        finding1Body:
          "Random 10% deletion maintains distribution shape, ensuring drift reflects realistic scenarios rather than introducing artificial bias",
        finding2Title: "Distribution Skewing",
        finding2Body:
          "For categorical attributes, upweighting frequent categories increases skew and tests optimizer robustness under non-uniform distributions",
      },
      workload: {
        title: "Census Workload: Drift Patterns",
        desc: "Predicate distribution changes and structural drift over time",
        setupTitle: "Workload Setup",
        setupBody:
          "We generate query workloads with varying predicate distributions (uniform, normal, skewed) and structural changes (additional predicates, payload modifications) to simulate evolving analytical demands.",
        changingTitle: "Changing Predicate Distributions",
        changingDesc: "Three workloads with different value distributions over time",
        phase1: "Phase 1",
        phase2: "Phase 2",
        phase3: "Phase 3",
        uniform: "Uniform Distribution",
        normal: "Normal Distribution",
        skewed: "Skewed Distribution",
        impactTitle: "Impact:",
        impactBody:
          "Query predicates follow uniform, normal, and skewed distributions across three timestamp groups. This reflects increasing levels of locality and skew, affecting learned indices and optimizers that rely on historical access patterns.",
        structuralTitle: "Structural Drift Analysis",
        structuralDesc: "Modifying query structure and payload over time",
        originalTemplates: "Original Templates",
        driftedTemplates: "Drifted Templates",
        originalItems: [
          "Max 5 predicates per query",
          "Max 6 projected columns",
          "Simple join conditions",
          "Lightweight aggregates",
        ],
        driftedItems: [
          "Max 7 predicates per query",
          "Max 8 projected columns",
          "Additional join tables",
          "Detailed record inspection",
        ],
        visualizationTitle: "Visualization:",
        visualizationBody:
          "Using t-SNE projection of query features reveals distinct clusters between original and drifted templates, demonstrating how logical and payload changes affect query structure space.",
      },
      estimator: {
        title: "Cardinality Estimator Evaluation Under Drift",
        desc: "Comparing PostgreSQL, Naru, and MSCN under data and workload drift",
        postgres: "PostgreSQL",
        naru: "Naru",
        mscn: "MSCN",
        ruleBased: "Rule-based",
        dataDriven: "Data-driven",
        dataQueryDriven: "Data & Query-driven",
        postgresDesc: "Traditional histogram-based estimator relying on column statistics",
        naruDesc: "Deep unsupervised learning approach for cardinality estimation",
        mscnDesc: "Learned model trained on both data and query features",
        qErrorDataTitle: "Q-Error Under Data Drift",
        qErrorDataDesc:
          "Fixed workload, dataset scaling from 1.0× to 3.0× (D₁ to D₁₀)",
        qErrorWorkloadTitle: "Q-Error Under Workload Drift",
        qErrorWorkloadDesc:
          "Fixed dataset, six-phase workload with varying predicates (W₁ to W₆)",
        workloadPhases:
          "Workload Phases: W₁ (age, workclass), W₂ (education, marital_status), W₃ (capital_gain, occupation), W₄ (hours_per_week, workclass), W₅ (capital_loss, marital_status), W₆ (age, native_country)",
        observationTitle: "Key Observation",
        observationData:
          "As dataset cardinality increases, the same query workload leads to larger true result sizes. Both PostgreSQL and learned models rely on stale statistics/models, causing Q-error to increase and outlier behavior to become more pronounced.",
        observationWorkload:
          "MSCN shows particularly strong sensitivity to query changes, as predictions vary substantially across workload phases. This is expected since MSCN is trained on query features. PostgreSQL and Naru, being primarily data-driven, exhibit more stable behavior across phases.",
        implicationsTitle: "Implications for Database Systems",
        implication1Title: "Statistics Staleness",
        implication1Body:
          "Traditional estimators degrade under cardinality drift without statistics updates. Automatic re-analysis or adaptive thresholds are needed.",
        implication2Title: "Model Adaptation",
        implication2Body:
          "Learned estimators require retraining or online adaptation mechanisms to maintain accuracy under evolving data and workload patterns.",
        implication3Title: "Drift-Aware Design",
        implication3Body:
          "Future database components should incorporate drift detection and adaptation as first-class features, moving beyond static optimization.",
      },
      labels: {
        originalPct: "Original (%)",
        afterDeletionPct: "After Deletion (%)",
        skewedPct: "Skewed (%)",
      },
    },
    generator: {
      title: "Drift Generator",
      subtitle:
        "Upload a dataset, configure drift parameters, and generate Data Drift and Workload Drift automatically",
      steps: ["Upload Data", "Configure Drift", "Preview", "Generate & Export"],
      uploadCsv: "Upload CSV File",
      uploadDesc: "Upload dataset from local file",
      clickUpload: "Click to upload or drag file",
      csvOnly: "CSV format supported",
      connectDb: "Connect Database",
      connectDesc: "Import from PostgreSQL database",
      connString: "Connection string",
      tableName: "Table name",
      connectBtn: "Connect Database",
      datasetInfo: "Dataset Info",
      dataset: "Dataset",
      totalColumns: "Total columns",
      numericColumns: "Numeric columns",
      categoricalColumns: "Categorical columns",
      columnName: "Column",
      columnType: "Type",
      distinctValues: "Distinct",
      samples: "Samples",
      dataDriftTab: "Data Drift",
      workloadDriftTab: "Workload Drift",
      temporalTab: "Temporal",
      dataDriftConfig: "Data Drift Configuration",
      dataDriftDesc: "Configure data drift types and parameters",
      driftType: "Drift type",
      cardinalityScaling: "Cardinality Scaling",
      cardinalityUpdating: "Cardinality Updating",
      columnShift: "Column Shift",
      outlierInjection: "Outlier Injection",
      scaleFactor: "Scale factor",
      scaleHelp: "Resize the dataset to {scale}×",
      deletePercent: "Delete percent",
      insertPercent: "Insert percent",
      updatingHelp: "Simulate continuous inserts and deletes",
      targetColumn: "Target column",
      skewness: "Skewness",
      outlierPercent: "Outlier percent (%)",
      outlierMultiplier: "Outlier multiplier",
      workloadDriftConfig: "Workload Drift Configuration",
      workloadDriftDesc: "Configure workload drift types and parameters",
      queryCount: "Query count",
      distributionType: "Distribution type",
      predicateDistribution: "Predicate Distribution",
      selectivityVariation: "Selectivity Variation",
      queryStructure: "Query Structure",
      payloadChange: "Payload Change",
      uniform: "Uniform",
      normal: "Normal",
      skewed: "Skewed",
      periodicLabel: "Periodic",
      trendLabel: "Trend",
      longTailLabel: "Long Tail",
      selectColumns: "Select query columns",
      selectivityRange: "Selectivity range",
      minValue: "Min (%)",
      maxValue: "Max (%)",
      selectivityHelp: "Query selectivity varies within this range",
      additionalPredicates: "Additional predicates",
      additionalHelp: "Extra predicates added to each query",
      projectionColumns: "Projection columns",
      projectionHelp: "Number of columns returned in SELECT",
      temporalConfig: "Temporal Pattern",
      temporalDesc: "Configure drift evolution over time",
      temporalPattern: "Temporal pattern",
      duration: "Duration (seconds)",
      temporalEnabledLabel: "Enable temporal mode",
      temporalDisabledHint: "Temporal mode is disabled. Drift will not include temporal settings.",
      uniformDesc: "Uniform: events arrive at a constant rate, good for baselines",
      periodicDesc: "Periodic: repeating cycles (daily/weekly patterns)",
      trendDesc: "Trend: gradual increase over time",
      longTailDesc: "Long Tail: burst followed by exponential decay",
      nextPreview: "Next: Preview",
      previewTitle: "Configuration Preview",
      previewDesc: "Review your drift configuration",
      enabled: "Enabled",
      disabled: "Disabled",
      driftSpecGenerated: "DriftSpec generated!",
      enableAtLeastOne: "Please enable at least one drift type",
      generateDriftSpec: "Generate DriftSpec",
      generatedSpec: "Generated DriftSpec",
      generatedDesc: "Configuration generated. Ready to download.",
      downloadYaml: "Download YAML",
      downloadScript: "Download Script",
      copyConfig: "Copy Config",
      nextSteps: "Next Steps",
      executionTarget: "Execution target",
      executionLocal: "Run locally",
      executionServer: "Run on server",
      executionLocalDesc: "Run the generated DriftSpec on your local machine.",
      executionServerDesc: "Submit the DriftSpec to the server for execution.",
      executeServer: "Execute on Server",
      serverNotConfigured: "Server execution is not configured yet.",
      step1Title: "Save the DriftSpec file",
      step1Body: "Save the generated YAML into your DriftBench project directory",
      step2Title: "Run DriftBench",
      step2Body: "Run the following command to generate drift:",
      step3Title: "View outputs",
      step3Body: "Generated data and queries are saved in the outputs/ directory",
      restart: "Start Over",
      modify: "Modify Configuration",
      dataDriftLabel: "Data Drift",
      workloadDriftLabel: "Workload Drift",
      temporalLabel: "Temporal Pattern",
      typeLabel: "Type",
      deleteLabel: "Delete",
      insertLabel: "Insert",
      unitCount: "items",
      unitColumns: "cols",
    },
  },
  zh: {
    common: {
      language: "语言",
      english: "English",
      chinese: "中文",
      previous: "上一步",
      next: "下一步",
      back: "返回",
    },
    nav: {
      home: "首页",
      driftTypes: "漂移类型",
      generator: "漂移生成器",
      visualization: "可视化",
      caseStudies: "案例研究",
      subtitle: "面向漂移的数据库基准测试",
      footerLine1: "基于 Guanli Liu 与 Renata Borovica-Gajic 的研究论文（墨尔本大学）",
      footerLink: "查看 GitHub",
    },
    home: {
      tagline: "面向漂移的数据库基准测试",
      title: "DriftBench",
      subtitle:
        "统一的漂移基准框架，用 DriftSpec 规范在数据和负载演化下评估数据库系统。",
      explore: "了解漂移类型",
      generate: "生成漂移",
      features: {
        taxonomyTitle: "漂移分类体系",
        taxonomyDesc: "统一刻画数据漂移、负载漂移与时间漂移",
        taxonomyItems: [
          "基数漂移（缩放、更新）",
          "分布漂移（列偏移、异常值）",
          "负载漂移（谓词、选择性、结构）",
          "时间模式（均匀、周期、趋势）",
        ],
        generatorTitle: "漂移生成器",
        generatorDesc: "上传数据集并配置漂移参数，生成 DriftSpec",
        generatorItems: [
          "上传 CSV 或连接数据库",
          "配置数据与负载漂移",
          "自动生成 DriftSpec",
          "下载 YAML 配置文件",
        ],
        frameworkTitle: "原型框架",
        frameworkDesc: "基于 DriftSpec 的合成与评估模块",
        frameworkItems: [
          "模式提取与分析",
          "分布模拟",
          "数据与负载生成",
          "时间漂移时间戳生成",
        ],
      },
      contributionsTitle: "关键贡献",
      contributions: [
        {
          title: "漂移成为一等概念",
          desc: "将基准测试从一次性评测转向可控、持续的漂移评估",
        },
        {
          title: "可执行的标准化语言",
          desc: "通过可复现的规范研究数据与负载演化对数据库行为的影响",
        },
        {
          title: "基准集成",
          desc: "兼容 TPC-H、RedBench 与自定义模式的基准或真实轨迹场景",
        },
        {
          title: "实际评估",
          desc: "通过基数估计器与学习索引的漂移案例研究验证",
        },
      ],
      ctaTitle: "准备开始了吗？",
      ctaDesc:
        "探索不同漂移类型，生成 DriftSpec 配置，并观察漂移对数据库组件的影响",
      ctaPrimary: "了解漂移类型",
      ctaSecondary: "查看可视化",
    },
    driftTypes: {
      title: "漂移分类",
      subtitle:
        "总结以往系统与基准中的常见模式，形成数据与负载漂移的分类体系。",
      tabs: {
        data: "数据漂移",
        workload: "负载漂移",
        temporal: "时间漂移",
      },
      data: {
        introTitle: "什么是数据漂移？",
        introDesc: "数据漂移指数据库记录数量或分布发生变化。",
        definition:
          "定义：D₁ 与 D₂ 表示同一模式 S 下两个版本的数据集。数据漂移即 D₁ 与 D₂ 在统计特性或规模上存在显著变化。",
        scalingTitle: "基数缩放",
        scalingDesc: "记录总量变化",
        cardinalityBadge: "基数漂移",
        scalingBody:
          "模拟数据集规模在新的快照下变化（例如 0.5×、2×、10×），不关注具体插入删除过程。",
        exampleLabel: "示例",
        scalingExample: "覆盖范围扩大或出生率增加导致人口记录增长",
        useCases: "应用场景",
        scalingUses: [
          "评估存储规模压力",
          "比较更大快照下的代价估计",
          "测试系统可扩展性（TPC-H、TPC-DS）",
        ],
        updatingTitle: "基数更新",
        updatingDesc: "按时间流持续插入与删除",
        updatingBody:
          "数据集随时间不断被修改，模拟持续插入与删除。",
        updatingExample: "出生、死亡、迁移引起人口记录持续变化",
        updatingUses: [
          "索引维护评估",
          "统计信息新鲜度测试",
          "持续更新行为分析",
        ],
        shiftingTitle: "列分布偏移",
        shiftingDesc: "列值分布变化",
        distributionBadge: "分布漂移",
        shiftingBody:
          "在不改变基数的情况下改变分布（如偏斜度增加），影响空间或时间负载。",
        shiftingExample: "招生季导致教育属性分布偏移",
        shiftingUses: [
          "空间负载评估",
          "选择性估计测试",
          "分布感知索引性能",
        ],
        outliersTitle: "异常值注入",
        outliersDesc: "通过极端值测试鲁棒性",
        outliersBody:
          "注入少量极端值以测试系统对异常分布或数据投毒的鲁棒性。",
        outliersExample: "极少量超大户籍或异常高收入记录",
        outliersUses: [
          "优化器鲁棒性测试",
          "学习索引投毒研究",
          "统计信息失真评估",
        ],
      },
      workload: {
        introTitle: "什么是负载漂移？",
        introDesc: "负载漂移指查询结构或统计特性随时间变化。",
        definition:
          "定义：负载 W 表示参数化模板 τ 生成的查询分布 P(Wτ(θ))，其中 θ ∈ Θ 表示参数生成器。",
        predicateTitle: "谓词分布变化",
        predicateDesc: "谓词随时间的统计分布变化",
        parametricBadge: "参数漂移",
        predicateBody:
          "谓词统计分布发生漂移，影响依赖历史访问模式的优化器与学习索引。",
        predicateExample: "查询逐渐集中在大城市而非均匀分布",
        impactLabel: "影响",
        predicateImpact: ["优化器准确性", "学习索引效果", "访问模式优化"],
        selectivityTitle: "选择性变化",
        selectivityDesc: "同一模板下范围变化",
        selectivityBody:
          "同一查询模板的谓词范围变化，导致不同的连接策略或执行计划。",
        selectivityExample: "查询不断扩大年龄范围导致选择性变化",
        selectivityImpact: ["计划稳定性", "连接策略选择", "基数估计准确性"],
        structureTitle: "查询结构变化",
        structureDesc: "模板与条件变化",
        structuralBadge: "结构漂移",
        structureBody:
          "修改模板或谓词条件，可能触发重新优化或改变索引使用。",
        structureExample: "查询增加额外的连接表与谓词",
        structureImpact: ["重新优化需求", "索引选择变化", "计划结构变化"],
        payloadTitle: "负载变化",
        payloadDesc: "投影列集合变化",
        payloadBody:
          "投影列集合变化将影响 I/O 成本与列扫描行为。",
        payloadExample: "从轻量聚合演进到详细记录查询",
        payloadImpact: ["I/O 成本变化", "列扫描行为", "输出模式变化"],
      },
      temporal: {
        introTitle: "什么是时间漂移？",
        introDesc: "时间漂移模拟随时间演化的非平稳模式，如突发、趋势或周期。",
        introBody:
          "我们采用 Sibyl 的四类时间模式，可独立使用或与数据/负载漂移组合。",
        uniformTitle: "均匀模式",
        uniformDesc: "恒定速率",
        uniformBody: "事件以恒定速率到达，适合基线对比。",
        periodicTitle: "周期模式",
        periodicDesc: "重复周期",
        periodicBody: "事件按周期重复，常见于日/周/季节性场景。",
        trendTitle: "趋势模式",
        trendDesc: "渐增或渐减",
        trendBody: "事件逐渐增加或减少，反映长期变化。",
        tailTitle: "长尾模式",
        tailDesc: "爆发后衰减",
        tailBody: "初期高峰后指数衰减，常见于热点事件。",
        combineTitle: "时间漂移与数据/负载漂移结合",
        combineBody: "时间模式可与数据与负载漂移组合以形成更丰富场景：",
        combineItems: [
          "查询时间戳：为每个实例生成时间戳，模拟真实查询流",
          "重复查询：典型的周期性查询可使用周期时间戳",
          "数据更新：插入/删除可按时间安排以模拟读写负载",
          "交互效应：观察突发或趋势下的性能退化",
        ],
      },
    },
    visualization: {
      title: "漂移可视化",
      subtitle: "交互式展示数据与负载漂移的不同模式",
      tabs: {
        cardinality: "基数漂移",
        distribution: "分布漂移",
        workload: "负载漂移",
        temporal: "时间漂移",
      },
      cardinality: {
        title: "基数缩放漂移",
        desc: "观察数据规模变化对分布的影响",
        scaleLabel: "缩放因子",
        records: "条记录",
        original: "原始数据集 (D₁)",
        scaled: "缩放数据集 (D₂)",
        impact:
          "规模变化影响存储开销、代价估计与统计信息准确性。分布形状相似，但体量会触发不同优化策略。",
      },
      distribution: {
        title: "渐进式分布漂移",
        desc: "观察分布随时间叠加式变化",
        stable: "稳定正态分布",
        newMode: "出现新模式（橙色）",
        outliers: "注入异常值（绿色）",
        columnTitle: "列分布偏移",
        columnDesc: "交互式调整偏斜度",
        skewness: "偏斜度",
        balanced: "均衡",
        moderate: "中等",
        heavy: "严重偏斜",
        original: "原始分布",
        skewed: "偏斜分布",
        impact:
          "分布漂移会显著影响基数估计与查询优化。偏斜度增加会让部分谓词更高选择性，其他谓词变得无效。",
      },
      workload: {
        title: "负载漂移模式",
        desc: "查询谓词分布随时间变化",
        original: "原始负载 (W₁)",
        drifted: "漂移负载 (W₂)",
        originalNote: "谓词值均匀分布（年龄范围中心）",
        driftedNote: "谓词范围向高值集中（参数漂移）",
        impact:
          "负载漂移会影响依赖历史模式的优化器与基数估计器，导致缓存统计或学习模型失效。",
      },
      temporal: {
        title: "时间漂移模式",
        desc: "观察漂移随时间演化",
        patternLabel: "时间模式",
        uniform: "均匀 - 恒定速率",
        periodic: "周期 - 重复波动",
        trend: "趋势 - 逐渐增长",
        tail: "长尾 - 爆发后衰减",
        axisTime: "时间（秒）",
        axisRate: "事件速率",
        uniformTitle: "均匀模式",
        periodicTitle: "周期模式",
        trendTitle: "趋势模式",
        tailTitle: "长尾模式",
        uniformBody: "事件以恒定速率到达，适合基线对比。",
        periodicBody: "事件呈周期波动，常见于日/周模式。",
        trendBody: "事件逐渐增加，体现长期增长。",
        tailBody: "初期高峰后指数衰减，常见于热点事件。",
        useCases: "应用场景",
        useCasesUniform: ["基线性能测试", "稳态系统行为", "可控实验设置"],
        useCasesPeriodic: ["业务高峰模拟", "季节性负载", "批处理周期"],
        useCasesTrend: ["用户增长模拟", "容量规划", "扩展性测试"],
        useCasesTail: ["病毒传播场景", "事件驱动负载", "突发流量"],
      },
      labels: {
        dataPoints: "数据点",
        datasetD1: "D₁",
        datasetD2New: "D₂ 新增",
        datasetD3Outliers: "D₃ 异常值",
        queries: "查询",
        eventRate: "事件速率",
        queryId: "查询编号",
        predicateCenter: "谓词中心",
      },
    },
    caseStudies: {
      title: "案例研究",
      subtitle: "在 Census 数据集上的 DriftBench 实验示例",
      researchTitle: "研究背景",
      researchBody:
        "这些案例来自论文“Toward Drift-Aware Database Benchmarking”（Guanli Liu 与 Renata Borovica-Gajic）。示例使用 Census 数据集展示 DriftBench 的漂移评估能力。",
      tabs: {
        data: "数据漂移",
        workload: "负载漂移",
        estimator: "估计器评估",
      },
      data: {
        title: "Census 数据集：数据漂移分析",
        desc: "分析基数更新与分布漂移",
        overviewTitle: "数据集概览",
        overviewBody:
          "Census Income 数据集包含年龄、工作类型、教育、婚姻、职业与收入等信息。我们选取代表性字段（年龄/工作类型）展示漂移影响。",
        updatingTitle: "基数更新",
        updatingDesc: "随机删除 10% 记录且保持分布",
        cardinalityBadge: "基数漂移",
        updatingObsTitle: "观察：",
        updatingObsBody:
          "删除子集按比例采样以保持原始分布形状，避免引入人工偏差。",
        shiftingTitle: "列分布偏移",
        shiftingDesc: "分类属性偏斜度增加",
        distBadge: "分布漂移",
        shiftingObsTitle: "观察：",
        shiftingObsBody:
          "高频类别（Private）占比进一步提升，导致选择性估计发生偏差。",
        keyFindings: "关键发现",
        finding1Title: "比例删除",
        finding1Body:
          "随机删除 10% 可保持分布形状，确保漂移更接近真实场景",
        finding2Title: "分布偏斜",
        finding2Body:
          "上调高频类别放大偏斜，用于测试优化器在非均匀分布下的鲁棒性",
      },
      workload: {
        title: "Census 负载：漂移模式",
        desc: "谓词分布变化与结构漂移",
        setupTitle: "负载设置",
        setupBody:
          "我们生成不同谓词分布（均匀/正态/偏态）与结构变化（额外谓词、负载变化）的查询，用于模拟需求演化。",
        changingTitle: "谓词分布变化",
        changingDesc: "三个阶段的不同分布",
        phase1: "阶段 1",
        phase2: "阶段 2",
        phase3: "阶段 3",
        uniform: "均匀分布",
        normal: "正态分布",
        skewed: "偏态分布",
        impactTitle: "影响：",
        impactBody:
          "谓词在三个时间段内分别呈现均匀、正态与偏态分布，体现访问局部性增强，影响学习索引与优化器。",
        structuralTitle: "结构漂移分析",
        structuralDesc: "查询结构与负载随时间变化",
        originalTemplates: "原始模板",
        driftedTemplates: "漂移模板",
        originalItems: [
          "每个查询最多 5 个谓词",
          "最多 6 个投影列",
          "简单连接条件",
          "轻量聚合",
        ],
        driftedItems: [
          "每个查询最多 7 个谓词",
          "最多 8 个投影列",
          "增加连接表",
          "更详细的记录查询",
        ],
        visualizationTitle: "可视化：",
        visualizationBody:
          "使用 t-SNE 将查询特征投影到平面，原始与漂移模板形成不同簇，展示结构变化带来的差异。",
      },
      estimator: {
        title: "漂移下的基数估计器评估",
        desc: "对比 PostgreSQL、Naru 与 MSCN",
        postgres: "PostgreSQL",
        naru: "Naru",
        mscn: "MSCN",
        ruleBased: "规则驱动",
        dataDriven: "数据驱动",
        dataQueryDriven: "数据+查询驱动",
        postgresDesc: "传统直方图估计器，依赖列统计信息",
        naruDesc: "无监督深度学习的基数估计方法",
        mscnDesc: "同时利用数据与查询特征的学习模型",
        qErrorDataTitle: "数据漂移下的 Q-Error",
        qErrorDataDesc: "固定负载，数据规模从 1.0× 扩展到 3.0×",
        qErrorWorkloadTitle: "负载漂移下的 Q-Error",
        qErrorWorkloadDesc: "固定数据集，六阶段不同谓词负载",
        workloadPhases:
          "负载阶段：W₁ (age, workclass), W₂ (education, marital_status), W₃ (capital_gain, occupation), W₄ (hours_per_week, workclass), W₅ (capital_loss, marital_status), W₆ (age, native_country)",
        observationTitle: "关键观察",
        observationData:
          "数据规模增大导致真实结果规模上升，统计与模型滞后使 Q-Error 上升并出现更多异常。",
        observationWorkload:
          "MSCN 对查询变化更敏感，因为其训练包含查询特征；PostgreSQL 与 Naru 在不同阶段表现更稳定。",
        implicationsTitle: "对数据库系统的启示",
        implication1Title: "统计信息滞后",
        implication1Body:
          "传统估计器在基数漂移下性能下降，需要自动重分析或自适应阈值。",
        implication2Title: "模型自适应",
        implication2Body:
          "学习型估计器需要重训练或在线自适应以维持准确度。",
        implication3Title: "漂移感知设计",
        implication3Body:
          "未来数据库组件应将漂移检测与自适应作为一等特性。",
      },
      labels: {
        originalPct: "原始 (%)",
        afterDeletionPct: "删除后 (%)",
        skewedPct: "偏斜 (%)",
      },
    },
    generator: {
      title: "漂移生成器",
      subtitle: "上传数据集，配置漂移参数，自动生成数据漂移与负载漂移",
      steps: ["上传数据", "配置漂移", "预览设置", "生成导出"],
      uploadCsv: "上传 CSV 文件",
      uploadDesc: "从本地上传数据集文件",
      clickUpload: "点击上传或拖拽文件",
      csvOnly: "支持 CSV 格式",
      connectDb: "连接数据库",
      connectDesc: "从 PostgreSQL 数据库导入",
      connString: "连接字符串",
      tableName: "表名",
      connectBtn: "连接数据库",
      datasetInfo: "数据集信息",
      dataset: "数据集",
      totalColumns: "总列数",
      numericColumns: "数值列",
      categoricalColumns: "分类列",
      columnName: "列名",
      columnType: "类型",
      distinctValues: "唯一值",
      samples: "示例",
      dataDriftTab: "数据漂移",
      workloadDriftTab: "负载漂移",
      temporalTab: "时间模式",
      dataDriftConfig: "数据漂移配置",
      dataDriftDesc: "配置数据漂移类型和参数",
      driftType: "漂移类型",
      cardinalityScaling: "基数缩放",
      cardinalityUpdating: "基数更新",
      columnShift: "列分布偏移",
      outlierInjection: "异常值注入",
      scaleFactor: "缩放因子",
      scaleHelp: "将数据集大小调整为原来的 {scale}×",
      deletePercent: "删除百分比",
      insertPercent: "插入百分比",
      updatingHelp: "模拟连续的插入和删除操作",
      targetColumn: "目标列",
      skewness: "偏度",
      outlierPercent: "异常值比例 (%)",
      outlierMultiplier: "异常值倍数",
      workloadDriftConfig: "负载漂移配置",
      workloadDriftDesc: "配置负载漂移类型和参数",
      queryCount: "查询数量",
      distributionType: "分布类型",
      predicateDistribution: "谓词分布变化",
      selectivityVariation: "选择性变化",
      queryStructure: "查询结构变化",
      payloadChange: "负载变化",
      uniform: "均匀分布",
      normal: "正态分布",
      skewed: "偏态分布",
      periodicLabel: "周期模式",
      trendLabel: "趋势模式",
      longTailLabel: "长尾模式",
      selectColumns: "选择查询列",
      selectivityRange: "选择性范围",
      minValue: "最小值 (%)",
      maxValue: "最大值 (%)",
      selectivityHelp: "查询选择性将在此范围内变化",
      additionalPredicates: "额外谓词数量",
      additionalHelp: "在原有查询基础上增加的谓词数量",
      projectionColumns: "投影列数量",
      projectionHelp: "SELECT 语句中返回的列数量",
      temporalConfig: "时间模式配置",
      temporalDesc: "配置漂移随时间的演化模式",
      temporalPattern: "时间模式",
      duration: "持续时间（秒）",
      temporalEnabledLabel: "启用时间模式",
      temporalDisabledHint: "时间模式已关闭，生成的漂移不包含时间设置。",
      uniformDesc: "均匀模式：事件以恒定速率到达，适合基准测试",
      periodicDesc: "周期模式：事件遵循重复的周期，模拟日常/每周模式",
      trendDesc: "趋势模式：事件逐渐增加，反映长期增长",
      longTailDesc: "长尾模式：高初始活动后指数衰减，模拟病毒式传播",
      nextPreview: "下一步：预览设置",
      previewTitle: "配置预览",
      previewDesc: "确认您的漂移配置",
      enabled: "已启用",
      disabled: "未启用",
      driftSpecGenerated: "DriftSpec 生成成功！",
      enableAtLeastOne: "请至少启用一种漂移类型",
      generateDriftSpec: "生成 DriftSpec",
      generatedSpec: "生成的 DriftSpec",
      generatedDesc: "配置文件已生成，可以下载使用",
      downloadYaml: "下载 YAML",
      downloadScript: "下载执行脚本",
      copyConfig: "复制配置",
      nextSteps: "下一步操作",
      executionTarget: "执行位置",
      executionLocal: "本地执行",
      executionServer: "服务器执行",
      executionLocalDesc: "在本地运行生成的 DriftSpec。",
      executionServerDesc: "将 DriftSpec 提交到服务器执行。",
      executeServer: "在服务器执行",
      serverNotConfigured: "服务器执行尚未配置。",
      step1Title: "保存 DriftSpec 文件",
      step1Body: "将生成的 YAML 配置保存到 DriftBench 项目目录中",
      step2Title: "运行 DriftBench",
      step2Body: "使用以下命令执行漂移生成：",
      step3Title: "查看输出结果",
      step3Body: "生成的数据和查询将保存在 outputs/ 目录中，可用于评估数据库系统",
      restart: "重新开始",
      modify: "修改配置",
      dataDriftLabel: "数据漂移",
      workloadDriftLabel: "负载漂移",
      temporalLabel: "时间模式",
      typeLabel: "类型",
      deleteLabel: "删除",
      insertLabel: "插入",
      unitCount: "个",
      unitColumns: "列",
    },
  },
} as const;

type Messages = typeof messages.en;

function getNested(obj: Record<string, any>, path: string): string | undefined {
  return path.split(".").reduce((acc, key) => {
    if (acc && typeof acc === "object" && key in acc) {
      return acc[key] as string | Record<string, any>;
    }
    return undefined;
  }, obj) as string | undefined;
}

type I18nContextValue = {
  lang: Language;
  setLang: (lang: Language) => void;
  t: (key: keyof Messages | string) => string;
  messages: Messages;
};

const I18nContext = React.createContext<I18nContextValue | null>(null);

export function LanguageProvider({ children }: { children: React.ReactNode }) {
  const [lang, setLang] = React.useState<Language>(() => {
    if (typeof window === "undefined") {
      return "en";
    }
    const stored = window.localStorage.getItem("wdg_lang");
    if (stored === "en" || stored === "zh") {
      return stored;
    }
    return "en";
  });

  React.useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("wdg_lang", lang);
    }
  }, [lang]);

  const value = React.useMemo<I18nContextValue>(() => {
    const currentMessages = messages[lang];
    return {
      lang,
      setLang,
      messages: currentMessages,
      t: (key: keyof Messages | string) => {
        const found = getNested(currentMessages as unknown as Record<string, any>, String(key));
        return found ?? String(key);
      },
    };
  }, [lang]);

  return (
    <I18nContext.Provider value={value}>
      {children}
    </I18nContext.Provider>
  );
}

export function useI18n() {
  const ctx = React.useContext(I18nContext);
  if (!ctx) {
    throw new Error("useI18n must be used within LanguageProvider");
  }
  return ctx;
}
