import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Label } from "../components/ui/label";
import { Input } from "../components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../components/ui/select";
import { Slider } from "../components/ui/slider";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Badge } from "../components/ui/badge";
import { Textarea } from "../components/ui/textarea";
import { Upload, Database, TrendingUp, Download, Play, FileText, Table, Activity } from "lucide-react";
import { toast } from "sonner";
import { Switch } from "../components/ui/switch";
import { Separator } from "../components/ui/separator";

type Column = {
  name: string;
  type: "numeric" | "categorical" | "text" | "date";
  samples: string[];
  min?: number;
  max?: number;
  distinct?: number;
};

type DataDriftConfig = {
  enabled: boolean;
  type: "cardinality_scaling" | "cardinality_updating" | "column_shift" | "outlier_injection";
  params: {
    scale?: number;
    deletePercent?: number;
    insertPercent?: number;
    column?: string;
    skewness?: number;
    outlierPercent?: number;
    outlierMultiplier?: number;
  };
};

type WorkloadDriftConfig = {
  enabled: boolean;
  type: "predicate_distribution" | "selectivity_variation" | "query_structure" | "payload_change";
  params: {
    queryCount?: number;
    distribution?: string;
    columns?: string[];
    selectivityRange?: [number, number];
    additionalPredicates?: number;
    projectionColumns?: number;
  };
};

export function DriftGenerator() {
  const [step, setStep] = useState(1);
  const [datasetName, setDatasetName] = useState("");
  const [columns, setColumns] = useState<Column[]>([]);
  const [dataDrift, setDataDrift] = useState<DataDriftConfig>({
    enabled: false,
    type: "cardinality_scaling",
    params: { scale: 2 },
  });
  const [workloadDrift, setWorkloadDrift] = useState<WorkloadDriftConfig>({
    enabled: false,
    type: "predicate_distribution",
    params: { queryCount: 100, distribution: "uniform", columns: [] },
  });
  const [temporalPattern, setTemporalPattern] = useState("uniform");
  const [duration, setDuration] = useState(600);

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setDatasetName(file.name);
      const mockColumns: Column[] = [
        {
          name: "age",
          type: "numeric",
          samples: ["18", "25", "34", "42", "56"],
          min: 18,
          max: 90,
          distinct: 73,
        },
        {
          name: "workclass",
          type: "categorical",
          samples: ["Private", "Self-emp-not-inc", "Local-gov", "State-gov", "Federal-gov"],
          distinct: 8,
        },
        {
          name: "education",
          type: "categorical",
          samples: ["Bachelors", "HS-grad", "11th", "Masters", "9th"],
          distinct: 16,
        },
        {
          name: "marital_status",
          type: "categorical",
          samples: ["Never-married", "Married-civ-spouse", "Divorced", "Separated", "Widowed"],
          distinct: 7,
        },
        {
          name: "occupation",
          type: "categorical",
          samples: ["Tech-support", "Craft-repair", "Other-service", "Sales", "Exec-managerial"],
          distinct: 14,
        },
        {
          name: "hours_per_week",
          type: "numeric",
          samples: ["40", "50", "35", "45", "60"],
          min: 1,
          max: 99,
          distinct: 96,
        },
        {
          name: "capital_gain",
          type: "numeric",
          samples: ["0", "2174", "14084", "5178", "15024"],
          min: 0,
          max: 99999,
          distinct: 119,
        },
        {
          name: "income",
          type: "categorical",
          samples: ["<=50K", ">50K"],
          distinct: 2,
        },
      ];
      setColumns(mockColumns);
      toast.success("数据集上传成功！已识别 " + mockColumns.length + " 个列");
      setStep(2);
    }
  };

  const handleDatabaseConnect = () => {
    setDatasetName("public.census");
    const mockColumns: Column[] = [
      {
        name: "age",
        type: "numeric",
        samples: ["18", "25", "34", "42", "56"],
        min: 18,
        max: 90,
        distinct: 73,
      },
      {
        name: "workclass",
        type: "categorical",
        samples: ["Private", "Self-emp-not-inc", "Local-gov"],
        distinct: 8,
      },
      {
        name: "education",
        type: "categorical",
        samples: ["Bachelors", "HS-grad", "11th"],
        distinct: 16,
      },
    ];
    setColumns(mockColumns);
    toast.success("数据库连接成功！");
    setStep(2);
  };

  const generateDriftSpec = () => {
    const specs: string[] = [];

    if (dataDrift.enabled) {
      const dataDriftSpec = `# Data Drift Configuration
pattern_id: data-drift-${Date.now()}
type:
  family: data
  category: ${dataDrift.type.includes("cardinality") ? "cardinality" : "distribution"}
  subtype: ${dataDrift.type}
data_source:
  kind: csv
  uri: './${datasetName}'
  output_path: 'outputs/${datasetName.replace(".csv", "")}_drifted.csv'
variables:
  params:
${dataDrift.type === "cardinality_scaling" ? `    scale_factor: ${dataDrift.params.scale}` : ""}
${dataDrift.type === "cardinality_updating"
    ? `    delete_percent: ${dataDrift.params.deletePercent}
    insert_percent: ${dataDrift.params.insertPercent}`
    : ""}
${dataDrift.type === "column_shift"
    ? `    column: ${dataDrift.params.column}
    skewness: ${dataDrift.params.skewness}`
    : ""}
${dataDrift.type === "outlier_injection"
    ? `    column: ${dataDrift.params.column}
    outlier_percent: ${dataDrift.params.outlierPercent}
    multiplier: ${dataDrift.params.outlierMultiplier}`
    : ""}
temporal:
  pattern: ${temporalPattern}
  duration_sec: ${duration}`;
      specs.push(dataDriftSpec);
    }

    if (workloadDrift.enabled) {
      const workloadDriftSpec = `# Workload Drift Configuration
pattern_id: workload-drift-${Date.now()}
type:
  family: workload
  category: ${workloadDrift.type.includes("predicate") || workloadDrift.type.includes("selectivity") ? "parametric" : "structural"}
  subtype: ${workloadDrift.type}
data_source:
  kind: csv
  uri: './${datasetName}'
  output_path: 'outputs/workload_queries.sql'
variables:
  params:
    query_count: ${workloadDrift.params.queryCount}
${workloadDrift.type === "predicate_distribution"
    ? `    distribution: ${workloadDrift.params.distribution}
    columns: [${workloadDrift.params.columns?.join(", ")}]`
    : ""}
${workloadDrift.type === "selectivity_variation"
    ? `    selectivity_range: [${workloadDrift.params.selectivityRange?.[0]}, ${workloadDrift.params.selectivityRange?.[1]}]`
    : ""}
${workloadDrift.type === "query_structure"
    ? `    additional_predicates: ${workloadDrift.params.additionalPredicates}`
    : ""}
${workloadDrift.type === "payload_change"
    ? `    projection_columns: ${workloadDrift.params.projectionColumns}`
    : ""}
temporal:
  pattern: ${temporalPattern}
  duration_sec: ${duration}`;
      specs.push(workloadDriftSpec);
    }

    return specs.join("\n\n---\n\n");
  };

  const handleGenerate = () => {
    if (!dataDrift.enabled && !workloadDrift.enabled) {
      toast.error("请至少启用一种漂移类型");
      return;
    }
    toast.success("DriftSpec 生成成功！");
    setStep(4);
  };

  const handleDownloadSpec = () => {
    const yaml = generateDriftSpec();
    const blob = new Blob([yaml], { type: "text/yaml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `drift-spec-${Date.now()}.yaml`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("DriftSpec 已下载！");
  };

  const handleDownloadScript = () => {
    const script = `#!/bin/bash
# DriftBench Execution Script
# Generated at ${new Date().toISOString()}

echo "Starting DriftBench..."

${dataDrift.enabled ? '# Generate Data Drift\npython driftbench.py generate --spec data-drift-spec.yaml\n' : ""}
${workloadDrift.enabled ? '# Generate Workload Drift\npython driftbench.py generate --spec workload-drift-spec.yaml\n' : ""}

echo "Drift generation completed!"
echo "Output files are available in the outputs/ directory"
`;
    const blob = new Blob([script], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `run-driftbench.sh`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("执行脚本已下载！");
  };

  const numericColumns = columns.filter((c) => c.type === "numeric");
  const categoricalColumns = columns.filter((c) => c.type === "categorical");

  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-4">漂移生成器</h1>
          <p className="text-lg text-muted-foreground">
            上传数据集，配置漂移参数，自动生成 Data Drift 和 Workload Drift
          </p>
        </div>

        <div className="flex items-center justify-between mb-8 max-w-2xl mx-auto">
          {[
            { num: 1, label: "上传数据" },
            { num: 2, label: "配置漂移" },
            { num: 3, label: "预览设置" },
            { num: 4, label: "生成导出" },
          ].map((s, idx) => (
            <div key={s.num} className="flex items-center flex-1">
              <div className="flex flex-col items-center flex-1">
                <div
                  className={`size-10 rounded-full flex items-center justify-center font-semibold ${
                    step >= s.num
                      ? "bg-primary text-primary-foreground"
                      : "bg-muted text-muted-foreground"
                  }`}
                >
                  {s.num}
                </div>
                <span className="text-xs mt-2">{s.label}</span>
              </div>
              {idx < 3 && (
                <div
                  className={`h-0.5 flex-1 mx-2 ${step > s.num ? "bg-primary" : "bg-muted"}`}
                />
              )}
            </div>
          ))}
        </div>

        {step === 1 && (
          <div className="grid md:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Upload className="size-5" />
                  上传 CSV 文件
                </CardTitle>
                <CardDescription>从本地上传数据集文件</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="border-2 border-dashed rounded-lg p-8 text-center hover:border-primary transition-colors cursor-pointer">
                  <Input
                    type="file"
                    accept=".csv"
                    className="hidden"
                    id="file-upload"
                    onChange={handleFileUpload}
                  />
                  <Label htmlFor="file-upload" className="cursor-pointer">
                    <Upload className="size-12 mx-auto mb-4 text-muted-foreground" />
                    <p className="text-sm font-medium mb-1">点击上传或拖拽文件</p>
                    <p className="text-xs text-muted-foreground">支持 CSV 格式</p>
                  </Label>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Database className="size-5" />
                  连接数据库
                </CardTitle>
                <CardDescription>从 PostgreSQL 数据库导入</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="db-uri">连接字符串</Label>
                  <Input
                    id="db-uri"
                    placeholder="postgresql://user:pass@host:5432/dbname"
                    defaultValue="postgresql://localhost:5432/census"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="table-name">表名</Label>
                  <Input id="table-name" placeholder="public.census" defaultValue="public.census" />
                </div>
                <Button onClick={handleDatabaseConnect} className="w-full">
                  连接数据库
                </Button>
              </CardContent>
            </Card>
          </div>
        )}

        {step === 2 && (
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Table className="size-5" />
                  数据集信息
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">数据集</p>
                    <p className="font-semibold truncate">{datasetName}</p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">总列数</p>
                    <p className="font-semibold">{columns.length} 列</p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">数值列</p>
                    <p className="font-semibold">{numericColumns.length} 列</p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">分类列</p>
                    <p className="font-semibold">{categoricalColumns.length} 列</p>
                  </div>
                </div>

                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-muted">
                      <tr>
                        <th className="text-left p-2 font-semibold">列名</th>
                        <th className="text-left p-2 font-semibold">类型</th>
                        <th className="text-left p-2 font-semibold">唯一值</th>
                        <th className="text-left p-2 font-semibold">示例</th>
                      </tr>
                    </thead>
                    <tbody>
                      {columns.slice(0, 5).map((col) => (
                        <tr key={col.name} className="border-t">
                          <td className="p-2 font-medium">{col.name}</td>
                          <td className="p-2">
                            <Badge variant="outline">{col.type}</Badge>
                          </td>
                          <td className="p-2">{col.distinct}</td>
                          <td className="p-2 text-xs text-muted-foreground">
                            {col.samples.slice(0, 3).join(", ")}...
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>

            <Tabs defaultValue="data-drift" className="w-full">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="data-drift">Data Drift</TabsTrigger>
                <TabsTrigger value="workload-drift">Workload Drift</TabsTrigger>
                <TabsTrigger value="temporal">时间模式</TabsTrigger>
              </TabsList>

              <TabsContent value="data-drift" className="space-y-6 mt-6">
                <Card>
                  <CardHeader>
                    <div className="flex items-center justify-between">
                      <div>
                        <CardTitle>Data Drift 配置</CardTitle>
                        <CardDescription>配置数据层面的漂移类型和参数</CardDescription>
                      </div>
                      <Switch
                        checked={dataDrift.enabled}
                        onCheckedChange={(checked) =>
                          setDataDrift({ ...dataDrift, enabled: checked })
                        }
                      />
                    </div>
                  </CardHeader>
                  {dataDrift.enabled && (
                    <CardContent className="space-y-6">
                      <div className="space-y-2">
                        <Label>漂移类型</Label>
                        <Select
                          value={dataDrift.type}
                          onValueChange={(value: DataDriftConfig["type"]) =>
                            setDataDrift({ ...dataDrift, type: value })
                          }
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="cardinality_scaling">
                              基数缩放 (Cardinality Scaling)
                            </SelectItem>
                            <SelectItem value="cardinality_updating">
                              基数更新 (Cardinality Updating)
                            </SelectItem>
                            <SelectItem value="column_shift">
                              列分布偏移 (Column Shift)
                            </SelectItem>
                            <SelectItem value="outlier_injection">
                              异常值注入 (Outlier Injection)
                            </SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <Separator />

                      {dataDrift.type === "cardinality_scaling" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>缩放因子</Label>
                              <span className="text-sm font-semibold">
                                {dataDrift.params.scale}×
                              </span>
                            </div>
                            <Slider
                              value={[dataDrift.params.scale || 2]}
                              onValueChange={([scale]) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, scale },
                                })
                              }
                              min={0.1}
                              max={10}
                              step={0.1}
                            />
                            <p className="text-xs text-muted-foreground mt-2">
                              将数据集大小调整为原来的 {dataDrift.params.scale}× 倍
                            </p>
                          </div>
                        </div>
                      )}

                      {dataDrift.type === "cardinality_updating" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>删除百分比</Label>
                              <span className="text-sm font-semibold">
                                {dataDrift.params.deletePercent}%
                              </span>
                            </div>
                            <Slider
                              value={[dataDrift.params.deletePercent || 10]}
                              onValueChange={([deletePercent]) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, deletePercent },
                                })
                              }
                              min={0}
                              max={50}
                              step={1}
                            />
                          </div>
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>插入百分比</Label>
                              <span className="text-sm font-semibold">
                                {dataDrift.params.insertPercent}%
                              </span>
                            </div>
                            <Slider
                              value={[dataDrift.params.insertPercent || 15]}
                              onValueChange={([insertPercent]) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, insertPercent },
                                })
                              }
                              min={0}
                              max={50}
                              step={1}
                            />
                          </div>
                          <p className="text-xs text-muted-foreground">
                            模拟连续的插入和删除操作
                          </p>
                        </div>
                      )}

                      {dataDrift.type === "column_shift" && (
                        <div className="space-y-4">
                          <div className="space-y-2">
                            <Label>目标列</Label>
                            <Select
                              value={dataDrift.params.column}
                              onValueChange={(column) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, column },
                                })
                              }
                            >
                              <SelectTrigger>
                                <SelectValue placeholder="选择列" />
                              </SelectTrigger>
                              <SelectContent>
                                {numericColumns.map((col) => (
                                  <SelectItem key={col.name} value={col.name}>
                                    {col.name}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>

                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>偏度</Label>
                              <span className="text-sm font-semibold">
                                {dataDrift.params.skewness?.toFixed(2)}
                              </span>
                            </div>
                            <Slider
                              value={[dataDrift.params.skewness || 0.5]}
                              onValueChange={([skewness]) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, skewness },
                                })
                              }
                              min={0}
                              max={2}
                              step={0.1}
                            />
                          </div>
                        </div>
                      )}

                      {dataDrift.type === "outlier_injection" && (
                        <div className="space-y-4">
                          <div className="space-y-2">
                            <Label>目标列</Label>
                            <Select
                              value={dataDrift.params.column}
                              onValueChange={(column) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, column },
                                })
                              }
                            >
                              <SelectTrigger>
                                <SelectValue placeholder="选择列" />
                              </SelectTrigger>
                              <SelectContent>
                                {numericColumns.map((col) => (
                                  <SelectItem key={col.name} value={col.name}>
                                    {col.name}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </div>

                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>异常值比例 (%)</Label>
                              <span className="text-sm font-semibold">
                                {dataDrift.params.outlierPercent}%
                              </span>
                            </div>
                            <Slider
                              value={[dataDrift.params.outlierPercent || 5]}
                              onValueChange={([outlierPercent]) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, outlierPercent },
                                })
                              }
                              min={1}
                              max={20}
                              step={1}
                            />
                          </div>

                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>异常值倍数</Label>
                              <span className="text-sm font-semibold">
                                {dataDrift.params.outlierMultiplier}×
                              </span>
                            </div>
                            <Slider
                              value={[dataDrift.params.outlierMultiplier || 3]}
                              onValueChange={([outlierMultiplier]) =>
                                setDataDrift({
                                  ...dataDrift,
                                  params: { ...dataDrift.params, outlierMultiplier },
                                })
                              }
                              min={2}
                              max={10}
                              step={0.5}
                            />
                          </div>
                        </div>
                      )}
                    </CardContent>
                  )}
                </Card>
              </TabsContent>

              <TabsContent value="workload-drift" className="space-y-6 mt-6">
                <Card>
                  <CardHeader>
                    <div className="flex items-center justify-between">
                      <div>
                        <CardTitle>Workload Drift 配置</CardTitle>
                        <CardDescription>配置工作负载漂移类型和参数</CardDescription>
                      </div>
                      <Switch
                        checked={workloadDrift.enabled}
                        onCheckedChange={(checked) =>
                          setWorkloadDrift({ ...workloadDrift, enabled: checked })
                        }
                      />
                    </div>
                  </CardHeader>
                  {workloadDrift.enabled && (
                    <CardContent className="space-y-6">
                      <div className="space-y-2">
                        <Label>漂移类型</Label>
                        <Select
                          value={workloadDrift.type}
                          onValueChange={(value: WorkloadDriftConfig["type"]) =>
                            setWorkloadDrift({ ...workloadDrift, type: value })
                          }
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="predicate_distribution">
                              谓词分布变化 (Predicate Distribution)
                            </SelectItem>
                            <SelectItem value="selectivity_variation">
                              选择性变化 (Selectivity Variation)
                            </SelectItem>
                            <SelectItem value="query_structure">
                              查询结构变化 (Query Structure)
                            </SelectItem>
                            <SelectItem value="payload_change">
                              负载变化 (Payload Change)
                            </SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <Separator />

                      <div>
                        <Label className="mb-2 block">查询数量</Label>
                        <Input
                          type="number"
                          value={workloadDrift.params.queryCount || 100}
                          onChange={(e) =>
                            setWorkloadDrift({
                              ...workloadDrift,
                              params: { ...workloadDrift.params, queryCount: parseInt(e.target.value) },
                            })
                          }
                          min={10}
                          max={1000}
                        />
                      </div>

                      {workloadDrift.type === "predicate_distribution" && (
                        <div className="space-y-4">
                          <div className="space-y-2">
                            <Label>分布类型</Label>
                            <Select
                              value={workloadDrift.params.distribution}
                              onValueChange={(distribution) =>
                                setWorkloadDrift({
                                  ...workloadDrift,
                                  params: { ...workloadDrift.params, distribution },
                                })
                              }
                            >
                              <SelectTrigger>
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="uniform">均匀分布 (Uniform)</SelectItem>
                                <SelectItem value="normal">正态分布 (Normal)</SelectItem>
                                <SelectItem value="skewed">偏态分布 (Skewed)</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>

                          <div>
                            <Label className="mb-2 block">选择查询列</Label>
                            <div className="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto border rounded-lg p-3">
                              {columns.map((col) => (
                                <div key={col.name} className="flex items-center space-x-2">
                                  <input
                                    type="checkbox"
                                    id={`col-${col.name}`}
                                    checked={workloadDrift.params.columns?.includes(col.name)}
                                    onChange={(e) => {
                                      const current = workloadDrift.params.columns || [];
                                      const updated = e.target.checked
                                        ? [...current, col.name]
                                        : current.filter((c) => c !== col.name);
                                      setWorkloadDrift({
                                        ...workloadDrift,
                                        params: { ...workloadDrift.params, columns: updated },
                                      });
                                    }}
                                    className="rounded"
                                  />
                                  <Label htmlFor={`col-${col.name}`} className="cursor-pointer">
                                    {col.name}
                                    <Badge variant="outline" className="ml-2">
                                      {col.type}
                                    </Badge>
                                  </Label>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>
                      )}

                      {workloadDrift.type === "selectivity_variation" && (
                        <div className="space-y-4">
                          <div>
                            <Label className="mb-2 block">选择性范围</Label>
                            <div className="grid grid-cols-2 gap-4">
                              <div className="space-y-2">
                                <Label className="text-xs">最小值 (%)</Label>
                                <Input
                                  type="number"
                                  value={workloadDrift.params.selectivityRange?.[0] || 1}
                                  onChange={(e) =>
                                    setWorkloadDrift({
                                      ...workloadDrift,
                                      params: {
                                        ...workloadDrift.params,
                                        selectivityRange: [
                                          parseFloat(e.target.value),
                                          workloadDrift.params.selectivityRange?.[1] || 50,
                                        ],
                                      },
                                    })
                                  }
                                  min={0.1}
                                  max={100}
                                  step={0.1}
                                />
                              </div>
                              <div className="space-y-2">
                                <Label className="text-xs">最大值 (%)</Label>
                                <Input
                                  type="number"
                                  value={workloadDrift.params.selectivityRange?.[1] || 50}
                                  onChange={(e) =>
                                    setWorkloadDrift({
                                      ...workloadDrift,
                                      params: {
                                        ...workloadDrift.params,
                                        selectivityRange: [
                                          workloadDrift.params.selectivityRange?.[0] || 1,
                                          parseFloat(e.target.value),
                                        ],
                                      },
                                    })
                                  }
                                  min={0.1}
                                  max={100}
                                  step={0.1}
                                />
                              </div>
                            </div>
                            <p className="text-xs text-muted-foreground mt-2">
                              查询的选择性将在此范围内变化
                            </p>
                          </div>
                        </div>
                      )}

                      {workloadDrift.type === "query_structure" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>额外谓词数量</Label>
                              <span className="text-sm font-semibold">
                                +{workloadDrift.params.additionalPredicates} 个
                              </span>
                            </div>
                            <Slider
                              value={[workloadDrift.params.additionalPredicates || 2]}
                              onValueChange={([additionalPredicates]) =>
                                setWorkloadDrift({
                                  ...workloadDrift,
                                  params: { ...workloadDrift.params, additionalPredicates },
                                })
                              }
                              min={1}
                              max={5}
                              step={1}
                            />
                            <p className="text-xs text-muted-foreground mt-2">
                              在原有查询基础上增加的谓词数量
                            </p>
                          </div>
                        </div>
                      )}

                      {workloadDrift.type === "payload_change" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>投影列数量</Label>
                              <span className="text-sm font-semibold">
                                {workloadDrift.params.projectionColumns} 列
                              </span>
                            </div>
                            <Slider
                              value={[workloadDrift.params.projectionColumns || 3]}
                              onValueChange={([projectionColumns]) =>
                                setWorkloadDrift({
                                  ...workloadDrift,
                                  params: { ...workloadDrift.params, projectionColumns },
                                })
                              }
                              min={1}
                              max={columns.length}
                              step={1}
                            />
                            <p className="text-xs text-muted-foreground mt-2">
                              SELECT 语句中返回的列数量
                            </p>
                          </div>
                        </div>
                      )}
                    </CardContent>
                  )}
                </Card>
              </TabsContent>

              <TabsContent value="temporal" className="space-y-6 mt-6">
                <Card>
                  <CardHeader>
                    <CardTitle>时间模式配置</CardTitle>
                    <CardDescription>配置漂移随时间的演化模式</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="space-y-2">
                      <Label>时间模式</Label>
                      <Select value={temporalPattern} onValueChange={setTemporalPattern}>
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="uniform">均匀模式 (Uniform)</SelectItem>
                          <SelectItem value="periodic">周期模式 (Periodic)</SelectItem>
                          <SelectItem value="trend">趋势模式 (Trend)</SelectItem>
                          <SelectItem value="long_tail">长尾模式 (Long Tail)</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>持续时间（秒）</Label>
                      <Input
                        type="number"
                        value={duration}
                        onChange={(e) => setDuration(parseInt(e.target.value))}
                      />
                    </div>
                    <div className="bg-muted p-4 rounded-lg">
                      <p className="text-sm">
                        {temporalPattern === "uniform" &&
                          "均匀模式：事件以恒定速率到达，适合基准测试"}
                        {temporalPattern === "periodic" &&
                          "周期模式：事件遵循重复的周期，模拟日常/每周模式"}
                        {temporalPattern === "trend" &&
                          "趋势模式：事件逐渐增加，反映长期增长"}
                        {temporalPattern === "long_tail" &&
                          "长尾模式：高初始活动后指数衰减，模拟病毒式传播"}
                      </p>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(1)}>
                上一步
              </Button>
              <Button onClick={() => setStep(3)}>下一步：预览设置</Button>
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>配置预览</CardTitle>
                <CardDescription>确认您的漂移配置</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-2 gap-6">
                  <div className="border rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <Database className="size-5 text-primary" />
                      <h3 className="font-semibold">Data Drift</h3>
                      {dataDrift.enabled ? (
                        <Badge>已启用</Badge>
                      ) : (
                        <Badge variant="secondary">未启用</Badge>
                      )}
                    </div>
                    {dataDrift.enabled ? (
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">类型:</span>
                          <span className="font-medium">
                            {dataDrift.type === "cardinality_scaling" && "基数缩放"}
                            {dataDrift.type === "cardinality_updating" && "基数更新"}
                            {dataDrift.type === "column_shift" && "列分布偏移"}
                            {dataDrift.type === "outlier_injection" && "异常值注入"}
                          </span>
                        </div>
                        {dataDrift.type === "cardinality_scaling" && (
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">缩放因子:</span>
                            <span className="font-medium">{dataDrift.params.scale}×</span>
                          </div>
                        )}
                        {dataDrift.type === "cardinality_updating" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">删除:</span>
                              <span className="font-medium">{dataDrift.params.deletePercent}%</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">插入:</span>
                              <span className="font-medium">{dataDrift.params.insertPercent}%</span>
                            </div>
                          </>
                        )}
                        {dataDrift.type === "column_shift" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">目标列:</span>
                              <span className="font-medium">{dataDrift.params.column}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">偏度:</span>
                              <span className="font-medium">
                                {dataDrift.params.skewness?.toFixed(2)}
                              </span>
                            </div>
                          </>
                        )}
                        {dataDrift.type === "outlier_injection" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">目标列:</span>
                              <span className="font-medium">{dataDrift.params.column}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">异常值比例:</span>
                              <span className="font-medium">
                                {dataDrift.params.outlierPercent}%
                              </span>
                            </div>
                          </>
                        )}
                      </div>
                    ) : (
                      <p className="text-sm text-muted-foreground">未启用数据漂移</p>
                    )}
                  </div>

                  <div className="border rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <Activity className="size-5 text-primary" />
                      <h3 className="font-semibold">Workload Drift</h3>
                      {workloadDrift.enabled ? (
                        <Badge>已启用</Badge>
                      ) : (
                        <Badge variant="secondary">未启用</Badge>
                      )}
                    </div>
                    {workloadDrift.enabled ? (
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">类型:</span>
                          <span className="font-medium">
                            {workloadDrift.type === "predicate_distribution" && "谓词分布变化"}
                            {workloadDrift.type === "selectivity_variation" && "选择性变化"}
                            {workloadDrift.type === "query_structure" && "查询结构变化"}
                            {workloadDrift.type === "payload_change" && "负载变化"}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">查询数量:</span>
                          <span className="font-medium">{workloadDrift.params.queryCount}</span>
                        </div>
                        {workloadDrift.type === "predicate_distribution" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">分布:</span>
                              <span className="font-medium">{workloadDrift.params.distribution}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">查询列:</span>
                              <span className="font-medium">
                                {workloadDrift.params.columns?.length || 0} 列
                              </span>
                            </div>
                          </>
                        )}
                      </div>
                    ) : (
                      <p className="text-sm text-muted-foreground">未启用工作负载漂移</p>
                    )}
                  </div>
                </div>

                <div className="border rounded-lg p-4">
                  <div className="flex items-center gap-2 mb-4">
                    <TrendingUp className="size-5 text-primary" />
                    <h3 className="font-semibold">时间模式</h3>
                  </div>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">模式:</span>
                      <span className="font-medium">{temporalPattern}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">持续时间:</span>
                      <span className="font-medium">{duration}s</span>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(2)}>
                上一步
              </Button>
              <Button onClick={handleGenerate}>
                <Play className="size-4 mr-2" />
                生成 DriftSpec
              </Button>
            </div>
          </div>
        )}

        {step === 4 && (
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <FileText className="size-5" />
                  生成的 DriftSpec
                </CardTitle>
                <CardDescription>配置文件已生成，可以下载使用</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <Textarea
                  value={generateDriftSpec()}
                  readOnly
                  className="font-mono text-sm h-96 resize-none"
                />

                <div className="grid md:grid-cols-3 gap-4">
                  <Button onClick={handleDownloadSpec} className="w-full">
                    <Download className="size-4 mr-2" />
                    下载 YAML
                  </Button>
                  <Button onClick={handleDownloadScript} variant="outline" className="w-full">
                    <Download className="size-4 mr-2" />
                    下载执行脚本
                  </Button>
                  <Button
                    onClick={() => {
                      navigator.clipboard.writeText(generateDriftSpec());
                      toast.success("已复制到剪贴板！");
                    }}
                    variant="outline"
                    className="w-full"
                  >
                    复制配置
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>下一步操作</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2 flex items-center gap-2">
                      <span className="size-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm">
                        1
                      </span>
                      保存 DriftSpec 文件
                    </h4>
                    <p className="text-sm text-muted-foreground ml-8">
                      将生成的 YAML 配置保存到您的 DriftBench 项目目录中
                    </p>
                  </div>

                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2 flex items-center gap-2">
                      <span className="size-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm">
                        2
                      </span>
                      运行 DriftBench
                    </h4>
                    <p className="text-sm text-muted-foreground ml-8 mb-2">
                      使用以下命令执行漂移生成：
                    </p>
                    <div className="bg-black text-green-400 p-3 rounded ml-8 font-mono text-xs">
                      $ python driftbench.py generate --spec drift-spec.yaml
                    </div>
                  </div>

                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2 flex items-center gap-2">
                      <span className="size-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm">
                        3
                      </span>
                      查看输出结果
                    </h4>
                    <p className="text-sm text-muted-foreground ml-8">
                      生成的数据和查询将保存在 outputs/ 目录中，可用于评估数据库系统
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(1)}>
                重新开始
              </Button>
              <Button variant="outline" onClick={() => setStep(2)}>
                修改配置
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
