import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Slider } from "../components/ui/slider";
import { Label } from "../components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../components/ui/select";
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, LineChart, Line, Legend } from "recharts";
import { useI18n } from "../i18n";

const generateNormalData = (count: number, meanX: number, meanY: number, stdDev: number) => {
  return Array.from({ length: count }, () => ({
    x: meanX + (Math.random() - 0.5) * 2 * stdDev * 3,
    y: meanY + (Math.random() - 0.5) * 2 * stdDev * 3,
  }));
};

export function Visualization() {
  const { t, messages } = useI18n();
  const [cardinalityScale, setCardinalityScale] = useState([2]);
  const [skewness, setSkewness] = useState([0.5]);
  const [temporalPattern, setTemporalPattern] = useState("uniform");

  const originalData = generateNormalData(200, 50, 50, 15);
  const scaledData = generateNormalData(200 * cardinalityScale[0], 50, 50, 15);

  const baseDistribution = generateNormalData(300, 0, 2, 1);
  const driftedDistribution1 = generateNormalData(150, 1, 2, 1.2);
  const driftedDistribution2 = [
    ...driftedDistribution1,
    ...generateNormalData(50, 3, 3, 0.8),
  ];

  const ageDistOriginal = Array.from({ length: 73 }, (_, i) => ({
    age: i + 18,
    frequency: Math.exp(-Math.pow((i + 18 - 40) / 15, 2)),
  }));

  const ageDistSkewed = Array.from({ length: 73 }, (_, i) => {
    const age = i + 18;
    const base = Math.exp(-Math.pow((age - 40) / 15, 2));
    const skew = skewness[0] > 0 ? Math.pow((90 - age) / 72, 1 / (1 + skewness[0])) : 1;
    return { age, frequency: base * skew };
  });

  const workloadOriginal = Array.from({ length: 100 }, (_, i) => ({
    query: i,
    predicateCenter: 45 + (Math.random() - 0.5) * 10,
  }));

  const workloadDrifted = Array.from({ length: 100 }, (_, i) => ({
    query: i,
    predicateCenter: 60 + Math.random() * 15 * (1 - i / 200),
  }));

  const generateTemporalData = (pattern: string) => {
    return Array.from({ length: 60 }, (_, i) => {
      let value = 0;
      switch (pattern) {
        case "uniform":
          value = 5;
          break;
        case "periodic":
          value = 5 + 3 * Math.sin((i / 10) * Math.PI);
          break;
        case "trend":
          value = 2 + (i / 60) * 6;
          break;
        case "long_tail":
          value = 8 * Math.exp(-i / 20);
          break;
      }
      return { time: i, rate: value };
    });
  };

  const temporalData = generateTemporalData(temporalPattern);

  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-4">{t("visualization.title")}</h1>
          <p className="text-lg text-muted-foreground">
            {t("visualization.subtitle")}
          </p>
        </div>

        <Tabs defaultValue="data-cardinality" className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="data-cardinality">{t("visualization.tabs.cardinality")}</TabsTrigger>
            <TabsTrigger value="data-distribution">{t("visualization.tabs.distribution")}</TabsTrigger>
            <TabsTrigger value="workload">{t("visualization.tabs.workload")}</TabsTrigger>
            <TabsTrigger value="temporal">{t("visualization.tabs.temporal")}</TabsTrigger>
          </TabsList>

          <TabsContent value="data-cardinality" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("visualization.cardinality.title")}</CardTitle>
                <CardDescription>
                  {t("visualization.cardinality.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <Label>
                      {t("visualization.cardinality.scaleLabel")}: {cardinalityScale[0]}x
                    </Label>
                    <span className="text-sm text-muted-foreground">
                      {originalData.length} → {scaledData.length} {t("visualization.cardinality.records")}
                    </span>
                  </div>
                  <Slider
                    value={cardinalityScale}
                    onValueChange={setCardinalityScale}
                    min={0.5}
                    max={5}
                    step={0.5}
                    className="w-full"
                  />
                </div>

                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.cardinality.original")}</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[0, 100]} />
                        <YAxis type="number" dataKey="y" domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name={t("visualization.labels.dataPoints")} data={originalData} fill="#3b82f6" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      Cardinality: {originalData.length} {t("visualization.cardinality.records")}
                    </p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.cardinality.scaled")}</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[0, 100]} />
                        <YAxis type="number" dataKey="y" domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name={t("visualization.labels.dataPoints")} data={scaledData} fill="#f59e0b" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      Cardinality: {scaledData.length} {t("visualization.cardinality.records")} ({cardinalityScale[0]}× scaling)
                    </p>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Impact:</strong> {t("visualization.cardinality.impact")}
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="data-distribution" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("visualization.distribution.title")}</CardTitle>
                <CardDescription>
                  {t("visualization.distribution.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-3 gap-4">
                  <div>
                    <h4 className="text-sm font-semibold mb-2">Dataset D₁</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[-4, 4]} />
                        <YAxis type="number" dataKey="y" domain={[-2, 6]} />
                        <Tooltip />
                        <Scatter name={t("visualization.labels.datasetD1")} data={baseDistribution} fill="#3b82f6" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">{t("visualization.distribution.stable")}</p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">Dataset D₂ (Cardinality Drift)</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[-4, 4]} />
                        <YAxis type="number" dataKey="y" domain={[-2, 6]} />
                        <Tooltip />
                        <Scatter name={t("visualization.labels.datasetD1")} data={baseDistribution} fill="#3b82f6" fillOpacity={0.3} />
                        <Scatter name={t("visualization.labels.datasetD2New")} data={driftedDistribution1} fill="#f59e0b" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">{t("visualization.distribution.newMode")}</p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">Dataset D₃ (Distributional Drift)</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[-4, 4]} />
                        <YAxis type="number" dataKey="y" domain={[-2, 6]} />
                        <Tooltip />
                        <Scatter name={`${t("visualization.labels.datasetD1")}+${t("visualization.labels.datasetD2New")}`} data={driftedDistribution1} fill="#f59e0b" fillOpacity={0.3} />
                        <Scatter
                          name={t("visualization.labels.datasetD3Outliers")}
                          data={driftedDistribution2.slice(-50)}
                          fill="#10b981"
                        />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">{t("visualization.distribution.outliers")}</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("visualization.distribution.columnTitle")}</CardTitle>
                <CardDescription>{t("visualization.distribution.columnDesc")}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <Label>{t("visualization.distribution.skewness")}: {skewness[0].toFixed(2)}</Label>
                    <span className="text-sm text-muted-foreground">
                      {skewness[0] < 0.3
                        ? t("visualization.distribution.balanced")
                        : skewness[0] < 0.7
                          ? t("visualization.distribution.moderate")
                          : t("visualization.distribution.heavy")}
                    </span>
                  </div>
                  <Slider
                    value={skewness}
                    onValueChange={setSkewness}
                    min={0}
                    max={2}
                    step={0.1}
                    className="w-full"
                  />
                </div>

                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.distribution.original")}</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <BarChart data={ageDistOriginal}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="age" />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="frequency" fill="#3b82f6" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.distribution.skewed")}</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <BarChart data={ageDistSkewed}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="age" />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="frequency" fill="#f59e0b" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Impact:</strong> {t("visualization.distribution.impact")}
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="workload" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("visualization.workload.title")}</CardTitle>
                <CardDescription>
                  {t("visualization.workload.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.workload.original")}</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="query" name={t("visualization.labels.queryId")} />
                        <YAxis type="number" dataKey="predicateCenter" name={t("visualization.labels.predicateCenter")} domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name={t("visualization.labels.queries")} data={workloadOriginal} fill="#3b82f6" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      {t("visualization.workload.originalNote")}
                    </p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.workload.drifted")}</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="query" name={t("visualization.labels.queryId")} />
                        <YAxis type="number" dataKey="predicateCenter" name={t("visualization.labels.predicateCenter")} domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name={t("visualization.labels.queries")} data={workloadDrifted} fill="#f59e0b" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      {t("visualization.workload.driftedNote")}
                    </p>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Impact:</strong> {t("visualization.workload.impact")}
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="temporal" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("visualization.temporal.title")}</CardTitle>
                <CardDescription>
                  {t("visualization.temporal.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-2">
                  <Label>{t("visualization.temporal.patternLabel")}</Label>
                  <Select value={temporalPattern} onValueChange={setTemporalPattern}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="uniform">{t("visualization.temporal.uniform")}</SelectItem>
                      <SelectItem value="periodic">{t("visualization.temporal.periodic")}</SelectItem>
                      <SelectItem value="trend">{t("visualization.temporal.trend")}</SelectItem>
                      <SelectItem value="long_tail">{t("visualization.temporal.tail")}</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <ResponsiveContainer width="100%" height={350}>
                  <LineChart data={temporalData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="time" label={{ value: t("visualization.temporal.axisTime"), position: "insideBottom", offset: -5 }} />
                    <YAxis label={{ value: t("visualization.temporal.axisRate"), angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="rate" stroke="#3b82f6" strokeWidth={2} name={t("visualization.labels.eventRate")} />
                  </LineChart>
                </ResponsiveContainer>

                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                    <h4 className="text-sm font-semibold mb-2 text-blue-900 dark:text-blue-100">
                      {temporalPattern === "uniform" && t("visualization.temporal.uniformTitle")}
                      {temporalPattern === "periodic" && t("visualization.temporal.periodicTitle")}
                      {temporalPattern === "trend" && t("visualization.temporal.trendTitle")}
                      {temporalPattern === "long_tail" && t("visualization.temporal.tailTitle")}
                    </h4>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      {temporalPattern === "uniform" && t("visualization.temporal.uniformBody")}
                      {temporalPattern === "periodic" && t("visualization.temporal.periodicBody")}
                      {temporalPattern === "trend" && t("visualization.temporal.trendBody")}
                      {temporalPattern === "long_tail" && t("visualization.temporal.tailBody")}
                    </p>
                  </div>

                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="text-sm font-semibold mb-2">{t("visualization.temporal.useCases")}</h4>
                    <ul className="text-sm space-y-1">
                      {temporalPattern === "uniform" &&
                        messages.visualization.temporal.useCasesUniform.map((item) => (
                          <li key={item}>• {item}</li>
                        ))}
                      {temporalPattern === "periodic" &&
                        messages.visualization.temporal.useCasesPeriodic.map((item) => (
                          <li key={item}>• {item}</li>
                        ))}
                      {temporalPattern === "trend" &&
                        messages.visualization.temporal.useCasesTrend.map((item) => (
                          <li key={item}>• {item}</li>
                        ))}
                      {temporalPattern === "long_tail" &&
                        messages.visualization.temporal.useCasesTail.map((item) => (
                          <li key={item}>• {item}</li>
                        ))}
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
