import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Slider } from "../components/ui/slider";
import { Label } from "../components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../components/ui/select";
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, LineChart, Line, Legend } from "recharts";

const generateNormalData = (count: number, meanX: number, meanY: number, stdDev: number) => {
  return Array.from({ length: count }, () => ({
    x: meanX + (Math.random() - 0.5) * 2 * stdDev * 3,
    y: meanY + (Math.random() - 0.5) * 2 * stdDev * 3,
  }));
};

export function Visualization() {
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
          <h1 className="text-4xl font-bold mb-4">Drift Visualization</h1>
          <p className="text-lg text-muted-foreground">
            Interactive visualizations demonstrating different types of data and workload drift
          </p>
        </div>

        <Tabs defaultValue="data-cardinality" className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="data-cardinality">Data Cardinality</TabsTrigger>
            <TabsTrigger value="data-distribution">Data Distribution</TabsTrigger>
            <TabsTrigger value="workload">Workload</TabsTrigger>
            <TabsTrigger value="temporal">Temporal</TabsTrigger>
          </TabsList>

          <TabsContent value="data-cardinality" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Cardinality Scaling Drift</CardTitle>
                <CardDescription>
                  Visualize how dataset size changes affect distribution
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <Label>Cardinality Scale Factor: {cardinalityScale[0]}x</Label>
                    <span className="text-sm text-muted-foreground">
                      {originalData.length} → {scaledData.length} records
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
                    <h4 className="text-sm font-semibold mb-2">Original Dataset (D₁)</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[0, 100]} />
                        <YAxis type="number" dataKey="y" domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name="Data Points" data={originalData} fill="#3b82f6" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      Cardinality: {originalData.length} records
                    </p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">Scaled Dataset (D₂)</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[0, 100]} />
                        <YAxis type="number" dataKey="y" domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name="Data Points" data={scaledData} fill="#f59e0b" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      Cardinality: {scaledData.length} records ({cardinalityScale[0]}× scaling)
                    </p>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Impact:</strong> Cardinality scaling affects storage footprint, query plan costs,
                    and statistics accuracy. The distribution pattern remains similar, but the sheer volume
                    can trigger different optimization strategies.
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="data-distribution" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Progressive Distribution Drift</CardTitle>
                <CardDescription>
                  Observe how data distribution evolves through overlapping shifts
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
                        <Scatter name="D₁" data={baseDistribution} fill="#3b82f6" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">Stable normal distribution</p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">Dataset D₂ (Cardinality Drift)</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[-4, 4]} />
                        <YAxis type="number" dataKey="y" domain={[-2, 6]} />
                        <Tooltip />
                        <Scatter name="D₁" data={baseDistribution} fill="#3b82f6" fillOpacity={0.3} />
                        <Scatter name="D₂ New" data={driftedDistribution1} fill="#f59e0b" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">New mode emerges (orange)</p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">Dataset D₃ (Distributional Drift)</h4>
                    <ResponsiveContainer width="100%" height={250}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="x" domain={[-4, 4]} />
                        <YAxis type="number" dataKey="y" domain={[-2, 6]} />
                        <Tooltip />
                        <Scatter name="D₁+D₂" data={driftedDistribution1} fill="#f59e0b" fillOpacity={0.3} />
                        <Scatter
                          name="D₃ Outliers"
                          data={driftedDistribution2.slice(-50)}
                          fill="#10b981"
                        />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">Outliers injected (green)</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Column Distribution Shift</CardTitle>
                <CardDescription>Interactive skewness adjustment</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <Label>Skewness: {skewness[0].toFixed(2)}</Label>
                    <span className="text-sm text-muted-foreground">
                      {skewness[0] < 0.3 ? "Balanced" : skewness[0] < 0.7 ? "Moderate" : "Heavy Skew"}
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
                    <h4 className="text-sm font-semibold mb-2">Original Distribution</h4>
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
                    <h4 className="text-sm font-semibold mb-2">Skewed Distribution</h4>
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
                    <strong>Impact:</strong> Distribution shifts can significantly affect cardinality estimation
                    and query optimization. Increased skewness concentrates data in specific ranges, potentially
                    making some predicates highly selective while others become ineffective.
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="workload" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Workload Drift Patterns</CardTitle>
                <CardDescription>
                  Changes in query predicate distributions over time
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <h4 className="text-sm font-semibold mb-2">Original Workload (W₁)</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="query" name="Query ID" />
                        <YAxis type="number" dataKey="predicateCenter" name="Predicate Center" domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name="Queries" data={workloadOriginal} fill="#3b82f6" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      Uniform distribution of predicate values (age range centers)
                    </p>
                  </div>

                  <div>
                    <h4 className="text-sm font-semibold mb-2">Drifted Workload (W₂)</h4>
                    <ResponsiveContainer width="100%" height={300}>
                      <ScatterChart>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" dataKey="query" name="Query ID" />
                        <YAxis type="number" dataKey="predicateCenter" name="Predicate Center" domain={[0, 100]} />
                        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                        <Scatter name="Queries" data={workloadDrifted} fill="#f59e0b" />
                      </ScatterChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground mt-2">
                      Predicate ranges shift and concentrate in higher values (parametric drift)
                    </p>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Impact:</strong> Workload drift affects learned query optimizers and cardinality
                    estimators that rely on historical query patterns. Shifting predicate distributions can
                    make cached statistics or learned models less accurate over time.
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="temporal" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Temporal Drift Patterns</CardTitle>
                <CardDescription>
                  How drift evolves over time with different patterns
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-2">
                  <Label>Temporal Pattern</Label>
                  <Select value={temporalPattern} onValueChange={setTemporalPattern}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="uniform">Uniform - Constant rate</SelectItem>
                      <SelectItem value="periodic">Periodic - Repeating cycles</SelectItem>
                      <SelectItem value="trend">Trend - Gradual increase</SelectItem>
                      <SelectItem value="long_tail">Long Tail - Burst with decay</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <ResponsiveContainer width="100%" height={350}>
                  <LineChart data={temporalData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="time" label={{ value: "Time (seconds)", position: "insideBottom", offset: -5 }} />
                    <YAxis label={{ value: "Event Rate", angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="rate" stroke="#3b82f6" strokeWidth={2} name="Event Rate" />
                  </LineChart>
                </ResponsiveContainer>

                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                    <h4 className="text-sm font-semibold mb-2 text-blue-900 dark:text-blue-100">
                      {temporalPattern === "uniform" && "Uniform Pattern"}
                      {temporalPattern === "periodic" && "Periodic Pattern"}
                      {temporalPattern === "trend" && "Trend Pattern"}
                      {temporalPattern === "long_tail" && "Long Tail Pattern"}
                    </h4>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      {temporalPattern === "uniform" &&
                        "Events arrive at a constant rate. Provides baseline for comparison."}
                      {temporalPattern === "periodic" &&
                        "Events follow repeating cycles with regular intervals. Common in daily/weekly patterns."}
                      {temporalPattern === "trend" &&
                        "Events show gradual increase over time. Reflects long-term growth."}
                      {temporalPattern === "long_tail" &&
                        "High initial activity followed by exponential decay. Common in viral events."}
                    </p>
                  </div>

                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="text-sm font-semibold mb-2">Use Cases</h4>
                    <ul className="text-sm space-y-1">
                      {temporalPattern === "uniform" && (
                        <>
                          <li>• Baseline performance testing</li>
                          <li>• Steady-state system behavior</li>
                          <li>• Controlled experiment setup</li>
                        </>
                      )}
                      {temporalPattern === "periodic" && (
                        <>
                          <li>• Business hours simulation</li>
                          <li>• Seasonal workload patterns</li>
                          <li>• Batch processing cycles</li>
                        </>
                      )}
                      {temporalPattern === "trend" && (
                        <>
                          <li>• Growing user base simulation</li>
                          <li>• Long-term capacity planning</li>
                          <li>• Scalability testing</li>
                        </>
                      )}
                      {temporalPattern === "long_tail" && (
                        <>
                          <li>• Viral content access patterns</li>
                          <li>• Event-driven workloads</li>
                          <li>• Flash crowd scenarios</li>
                        </>
                      )}
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
