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
import { useI18n } from "../i18n";

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
  const { t, messages } = useI18n();
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
  const [temporalEnabled, setTemporalEnabled] = useState(false);
  const [temporalPattern, setTemporalPattern] = useState("uniform");
  const [duration, setDuration] = useState(600);
  const [executionTarget, setExecutionTarget] = useState<"local" | "server">("local");
  const temporalPatternLabel =
    temporalPattern === "uniform"
      ? t("generator.uniform")
      : temporalPattern === "periodic"
        ? t("generator.periodicLabel")
        : temporalPattern === "trend"
          ? t("generator.trendLabel")
          : t("generator.longTailLabel");

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
      toast.success(`${t("generator.datasetInfo")} ${mockColumns.length}`);
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
    toast.success(t("generator.connectBtn"));
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
${temporalEnabled ? `temporal:
  pattern: ${temporalPattern}
  duration_sec: ${duration}` : ""}`;
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
${temporalEnabled ? `temporal:
  pattern: ${temporalPattern}
  duration_sec: ${duration}` : ""}`;
      specs.push(workloadDriftSpec);
    }

    return specs.join("\n\n---\n\n");
  };

  const handleGenerate = () => {
    if (!dataDrift.enabled && !workloadDrift.enabled) {
      toast.error(t("generator.enableAtLeastOne"));
      return;
    }
    toast.success(t("generator.driftSpecGenerated"));
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
    toast.success(t("generator.downloadYaml"));
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
    toast.success(t("generator.downloadScript"));
  };

  const numericColumns = columns.filter((c) => c.type === "numeric");
  const categoricalColumns = columns.filter((c) => c.type === "categorical");

  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-4">{t("generator.title")}</h1>
          <p className="text-lg text-muted-foreground">{t("generator.subtitle")}</p>
        </div>

        <div className="flex items-center justify-between mb-8 max-w-2xl mx-auto">
          {[
            { num: 1, label: messages.generator.steps[0] },
            { num: 2, label: messages.generator.steps[1] },
            { num: 3, label: messages.generator.steps[2] },
            { num: 4, label: messages.generator.steps[3] },
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
                  {t("generator.uploadCsv")}
                </CardTitle>
                <CardDescription>{t("generator.uploadDesc")}</CardDescription>
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
                    <p className="text-sm font-medium mb-1">{t("generator.clickUpload")}</p>
                    <p className="text-xs text-muted-foreground">{t("generator.csvOnly")}</p>
                  </Label>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Database className="size-5" />
                  {t("generator.connectDb")}
                </CardTitle>
                <CardDescription>{t("generator.connectDesc")}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="db-uri">{t("generator.connString")}</Label>
                  <Input
                    id="db-uri"
                    placeholder="postgresql://user:pass@host:5432/dbname"
                    defaultValue="postgresql://localhost:5432/census"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="table-name">{t("generator.tableName")}</Label>
                  <Input id="table-name" placeholder="public.census" defaultValue="public.census" />
                </div>
                <Button onClick={handleDatabaseConnect} className="w-full">
                  {t("generator.connectBtn")}
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
                  {t("generator.datasetInfo")}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">{t("generator.dataset")}</p>
                    <p className="font-semibold truncate">{datasetName}</p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">{t("generator.totalColumns")}</p>
                    <p className="font-semibold">{columns.length}</p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">{t("generator.numericColumns")}</p>
                    <p className="font-semibold">{numericColumns.length}</p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs text-muted-foreground mb-1">{t("generator.categoricalColumns")}</p>
                    <p className="font-semibold">{categoricalColumns.length}</p>
                  </div>
                </div>

                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-muted">
                      <tr>
                        <th className="text-left p-2 font-semibold">{t("generator.columnName")}</th>
                        <th className="text-left p-2 font-semibold">{t("generator.columnType")}</th>
                        <th className="text-left p-2 font-semibold">{t("generator.distinctValues")}</th>
                        <th className="text-left p-2 font-semibold">{t("generator.samples")}</th>
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
                <TabsTrigger value="data-drift">{t("generator.dataDriftTab")}</TabsTrigger>
                <TabsTrigger value="workload-drift">{t("generator.workloadDriftTab")}</TabsTrigger>
                <TabsTrigger value="temporal">{t("generator.temporalTab")}</TabsTrigger>
              </TabsList>

              <TabsContent value="data-drift" className="space-y-6 mt-6">
                <Card>
                  <CardHeader>
                    <div className="flex items-center justify-between">
                      <div>
                        <CardTitle>{t("generator.dataDriftConfig")}</CardTitle>
                        <CardDescription>{t("generator.dataDriftDesc")}</CardDescription>
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
                        <Label>{t("generator.driftType")}</Label>
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
                              {t("generator.cardinalityScaling")}
                            </SelectItem>
                            <SelectItem value="cardinality_updating">
                              {t("generator.cardinalityUpdating")}
                            </SelectItem>
                            <SelectItem value="column_shift">
                              {t("generator.columnShift")}
                            </SelectItem>
                            <SelectItem value="outlier_injection">
                              {t("generator.outlierInjection")}
                            </SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <Separator />

                      {dataDrift.type === "cardinality_scaling" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>{t("generator.scaleFactor")}</Label>
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
                              {t("generator.scaleHelp").replace("{scale}", String(dataDrift.params.scale))}
                            </p>
                          </div>
                        </div>
                      )}

                      {dataDrift.type === "cardinality_updating" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>{t("generator.deletePercent")}</Label>
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
                              <Label>{t("generator.insertPercent")}</Label>
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
                            {t("generator.updatingHelp")}
                          </p>
                        </div>
                      )}

                      {dataDrift.type === "column_shift" && (
                        <div className="space-y-4">
                          <div className="space-y-2">
                            <Label>{t("generator.targetColumn")}</Label>
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
                              <SelectValue placeholder={t("generator.targetColumn")} />
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
                              <Label>{t("generator.skewness")}</Label>
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
                            <Label>{t("generator.targetColumn")}</Label>
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
                              <SelectValue placeholder={t("generator.targetColumn")} />
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
                              <Label>{t("generator.outlierPercent")}</Label>
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
                              <Label>{t("generator.outlierMultiplier")}</Label>
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
                        <CardTitle>{t("generator.workloadDriftConfig")}</CardTitle>
                        <CardDescription>{t("generator.workloadDriftDesc")}</CardDescription>
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
                        <Label>{t("generator.driftType")}</Label>
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
                              {t("generator.predicateDistribution")}
                            </SelectItem>
                            <SelectItem value="selectivity_variation">
                              {t("generator.selectivityVariation")}
                            </SelectItem>
                            <SelectItem value="query_structure">
                              {t("generator.queryStructure")}
                            </SelectItem>
                            <SelectItem value="payload_change">
                              {t("generator.payloadChange")}
                            </SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      <Separator />

                      <div>
                        <Label className="mb-2 block">{t("generator.queryCount")}</Label>
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
                            <Label>{t("generator.distributionType")}</Label>
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
                              <SelectItem value="uniform">{t("generator.uniform")}</SelectItem>
                              <SelectItem value="normal">{t("generator.normal")}</SelectItem>
                              <SelectItem value="skewed">{t("generator.skewed")}</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>

                          <div>
                            <Label className="mb-2 block">{t("generator.selectColumns")}</Label>
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
                            <Label className="mb-2 block">{t("generator.selectivityRange")}</Label>
                            <div className="grid grid-cols-2 gap-4">
                              <div className="space-y-2">
                                <Label className="text-xs">{t("generator.minValue")}</Label>
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
                                <Label className="text-xs">{t("generator.maxValue")}</Label>
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
                              {t("generator.selectivityHelp")}
                            </p>
                          </div>
                        </div>
                      )}

                      {workloadDrift.type === "query_structure" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>{t("generator.additionalPredicates")}</Label>
                              <span className="text-sm font-semibold">
                                +{workloadDrift.params.additionalPredicates} {t("generator.unitCount")}
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
                              {t("generator.additionalHelp")}
                            </p>
                          </div>
                        </div>
                      )}

                      {workloadDrift.type === "payload_change" && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <Label>{t("generator.projectionColumns")}</Label>
                              <span className="text-sm font-semibold">
                                {workloadDrift.params.projectionColumns} {t("generator.unitColumns")}
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
                              {t("generator.projectionHelp")}
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
                    <div className="flex items-center justify-between">
                      <div>
                        <CardTitle>{t("generator.temporalConfig")}</CardTitle>
                        <CardDescription>{t("generator.temporalDesc")}</CardDescription>
                      </div>
                      <div className="flex items-center gap-2">
                        <Label className="text-xs text-muted-foreground">
                          {t("generator.temporalEnabledLabel")}
                        </Label>
                        <Switch
                          checked={temporalEnabled}
                          onCheckedChange={(checked) => setTemporalEnabled(checked)}
                        />
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    {!temporalEnabled && (
                      <p className="text-sm text-muted-foreground">
                        {t("generator.temporalDisabledHint")}
                      </p>
                    )}
                    {temporalEnabled && (
                      <>
                        <div className="space-y-2">
                          <Label>{t("generator.temporalPattern")}</Label>
                          <Select value={temporalPattern} onValueChange={setTemporalPattern}>
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="uniform">{t("generator.uniform")}</SelectItem>
                              <SelectItem value="periodic">{t("generator.periodicLabel")}</SelectItem>
                              <SelectItem value="trend">{t("generator.trendLabel")}</SelectItem>
                              <SelectItem value="long_tail">{t("generator.longTailLabel")}</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label>{t("generator.duration")}</Label>
                          <Input
                            type="number"
                            value={duration}
                            onChange={(e) => setDuration(parseInt(e.target.value))}
                          />
                        </div>
                        <div className="bg-muted p-4 rounded-lg">
                          <p className="text-sm">
                            {temporalPattern === "uniform" && t("generator.uniformDesc")}
                            {temporalPattern === "periodic" && t("generator.periodicDesc")}
                            {temporalPattern === "trend" && t("generator.trendDesc")}
                            {temporalPattern === "long_tail" && t("generator.longTailDesc")}
                          </p>
                        </div>
                      </>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(1)}>
                {t("common.previous")}
              </Button>
              <Button onClick={() => setStep(3)}>{t("generator.nextPreview")}</Button>
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("generator.previewTitle")}</CardTitle>
                <CardDescription>{t("generator.previewDesc")}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-2 gap-6">
                  <div className="border rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <Database className="size-5 text-primary" />
                      <h3 className="font-semibold">{t("generator.dataDriftLabel")}</h3>
                      {dataDrift.enabled ? (
                        <Badge>{t("generator.enabled")}</Badge>
                      ) : (
                        <Badge variant="secondary">{t("generator.disabled")}</Badge>
                      )}
                    </div>
                    {dataDrift.enabled ? (
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">{t("generator.typeLabel")}:</span>
                          <span className="font-medium">
                            {dataDrift.type === "cardinality_scaling" && t("generator.cardinalityScaling")}
                            {dataDrift.type === "cardinality_updating" && t("generator.cardinalityUpdating")}
                            {dataDrift.type === "column_shift" && t("generator.columnShift")}
                            {dataDrift.type === "outlier_injection" && t("generator.outlierInjection")}
                          </span>
                        </div>
                        {dataDrift.type === "cardinality_scaling" && (
                          <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.scaleFactor")}:</span>
                            <span className="font-medium">{dataDrift.params.scale}×</span>
                          </div>
                        )}
                        {dataDrift.type === "cardinality_updating" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.deleteLabel")}:</span>
                              <span className="font-medium">{dataDrift.params.deletePercent}%</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.insertLabel")}:</span>
                              <span className="font-medium">{dataDrift.params.insertPercent}%</span>
                            </div>
                          </>
                        )}
                        {dataDrift.type === "column_shift" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.targetColumn")}:</span>
                              <span className="font-medium">{dataDrift.params.column}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.skewness")}:</span>
                              <span className="font-medium">
                                {dataDrift.params.skewness?.toFixed(2)}
                              </span>
                            </div>
                          </>
                        )}
                        {dataDrift.type === "outlier_injection" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.targetColumn")}:</span>
                              <span className="font-medium">{dataDrift.params.column}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.outlierPercent")}:</span>
                              <span className="font-medium">
                                {dataDrift.params.outlierPercent}%
                              </span>
                            </div>
                          </>
                        )}
                      </div>
                    ) : (
                      <p className="text-sm text-muted-foreground">{t("generator.disabled")}</p>
                    )}
                  </div>

                  <div className="border rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <Activity className="size-5 text-primary" />
                      <h3 className="font-semibold">{t("generator.workloadDriftLabel")}</h3>
                      {workloadDrift.enabled ? (
                        <Badge>{t("generator.enabled")}</Badge>
                      ) : (
                        <Badge variant="secondary">{t("generator.disabled")}</Badge>
                      )}
                    </div>
                    {workloadDrift.enabled ? (
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">{t("generator.typeLabel")}:</span>
                          <span className="font-medium">
                            {workloadDrift.type === "predicate_distribution" && t("generator.predicateDistribution")}
                            {workloadDrift.type === "selectivity_variation" && t("generator.selectivityVariation")}
                            {workloadDrift.type === "query_structure" && t("generator.queryStructure")}
                            {workloadDrift.type === "payload_change" && t("generator.payloadChange")}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">{t("generator.queryCount")}:</span>
                          <span className="font-medium">{workloadDrift.params.queryCount}</span>
                        </div>
                        {workloadDrift.type === "predicate_distribution" && (
                          <>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.distributionType")}:</span>
                              <span className="font-medium">{workloadDrift.params.distribution}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">{t("generator.selectColumns")}:</span>
                              <span className="font-medium">
                                {workloadDrift.params.columns?.length || 0} {t("generator.unitColumns")}
                              </span>
                            </div>
                          </>
                        )}
                      </div>
                    ) : (
                      <p className="text-sm text-muted-foreground">{t("generator.disabled")}</p>
                    )}
                  </div>
                </div>

                <div className="border rounded-lg p-4">
                  <div className="flex items-center gap-2 mb-4">
                    <TrendingUp className="size-5 text-primary" />
                    <h3 className="font-semibold">{t("generator.temporalLabel")}</h3>
                    {temporalEnabled ? (
                      <Badge>{t("generator.enabled")}</Badge>
                    ) : (
                      <Badge variant="secondary">{t("generator.disabled")}</Badge>
                    )}
                  </div>
                  {temporalEnabled ? (
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">{t("generator.temporalPattern")}:</span>
                        <span className="font-medium">{temporalPatternLabel}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">{t("generator.duration")}:</span>
                        <span className="font-medium">{duration}s</span>
                      </div>
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      {t("generator.temporalDisabledHint")}
                    </p>
                  )}
                </div>
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(2)}>
                {t("common.previous")}
              </Button>
              <Button onClick={handleGenerate}>
                <Play className="size-4 mr-2" />
                {t("generator.generateDriftSpec")}
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
                  {t("generator.generatedSpec")}
                </CardTitle>
                <CardDescription>{t("generator.generatedDesc")}</CardDescription>
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
                    {t("generator.downloadYaml")}
                  </Button>
                  <Button onClick={handleDownloadScript} variant="outline" className="w-full">
                    <Download className="size-4 mr-2" />
                    {t("generator.downloadScript")}
                  </Button>
                  <Button
                    onClick={() => {
                      navigator.clipboard.writeText(generateDriftSpec());
                      toast.success(t("generator.copyConfig"));
                    }}
                    variant="outline"
                    className="w-full"
                  >
                    {t("generator.copyConfig")}
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("generator.nextSteps")}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="mb-6 space-y-2">
                  <Label>{t("generator.executionTarget")}</Label>
                  <Select
                    value={executionTarget}
                    onValueChange={(value) => setExecutionTarget(value as "local" | "server")}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="local">{t("generator.executionLocal")}</SelectItem>
                      <SelectItem value="server">{t("generator.executionServer")}</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    {executionTarget === "local"
                      ? t("generator.executionLocalDesc")
                      : t("generator.executionServerDesc")}
                  </p>
                </div>

                <div className="space-y-4">
                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2 flex items-center gap-2">
                      <span className="size-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm">
                        1
                      </span>
                      {t("generator.step1Title")}
                    </h4>
                    <p className="text-sm text-muted-foreground ml-8">
                      {t("generator.step1Body")}
                    </p>
                  </div>

                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2 flex items-center gap-2">
                      <span className="size-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm">
                        2
                      </span>
                      {t("generator.step2Title")}
                    </h4>
                    {executionTarget === "local" ? (
                      <>
                        <p className="text-sm text-muted-foreground ml-8 mb-2">
                          {t("generator.step2Body")}
                        </p>
                        <div className="bg-black text-green-400 p-3 rounded ml-8 font-mono text-xs">
                          $ python driftbench.py generate --spec drift-spec.yaml
                        </div>
                      </>
                    ) : (
                      <>
                        <p className="text-sm text-muted-foreground ml-8 mb-3">
                          {t("generator.executionServerDesc")}
                        </p>
                        <div className="ml-8">
                          <Button
                            variant="outline"
                            onClick={() => toast.info(t("generator.serverNotConfigured"))}
                          >
                            {t("generator.executeServer")}
                          </Button>
                        </div>
                      </>
                    )}
                  </div>

                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2 flex items-center gap-2">
                      <span className="size-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm">
                        3
                      </span>
                      {t("generator.step3Title")}
                    </h4>
                    <p className="text-sm text-muted-foreground ml-8">
                      {t("generator.step3Body")}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(1)}>
                {t("generator.restart")}
              </Button>
              <Button variant="outline" onClick={() => setStep(2)}>
                {t("generator.modify")}
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
