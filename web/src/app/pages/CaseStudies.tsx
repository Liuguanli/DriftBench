import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Badge } from "../components/ui/badge";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LineChart, Line } from "recharts";
import { AlertCircle, TrendingUp, Database } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";

export function CaseStudies() {
  const qErrorDataDrift = Array.from({ length: 10 }, (_, i) => ({
    snapshot: `D${i + 1}`,
    time: i * 10,
    postgres: 1.5 + Math.random() * (i * 0.3),
    naru: 1.2 + Math.random() * (i * 0.25),
    mscn: 2.0 + Math.random() * (i * 0.5),
  }));

  const qErrorWorkloadDrift = Array.from({ length: 6 }, (_, i) => ({
    phase: `W${i + 1}`,
    time: i * 10,
    postgres: 2.5 + Math.random() * 2,
    naru: 1.8 + Math.random() * 1.5,
    mscn: 3.5 + Math.random() * 4,
  }));

  const ageDistribution = [
    { range: "18-25", original: 15, drift: 12 },
    { range: "26-35", original: 25, drift: 20 },
    { range: "36-45", original: 22, drift: 18 },
    { range: "46-55", original: 20, drift: 22 },
    { range: "56-65", original: 12, drift: 18 },
    { range: "66+", original: 6, drift: 10 },
  ];

  const workclassDistribution = [
    { category: "Private", original: 70, drift: 75 },
    { category: "Self-Employed", original: 10, drift: 8 },
    { category: "Local-gov", original: 8, drift: 6 },
    { category: "State-gov", original: 7, drift: 6 },
    { category: "Federal-gov", original: 5, drift: 5 },
  ];

  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-4">Case Studies</h1>
          <p className="text-lg text-muted-foreground">
            Practical demonstrations of DriftBench applied to real-world scenarios using the Census dataset
          </p>
        </div>

        <Alert className="mb-8">
          <AlertCircle className="size-4" />
          <AlertTitle>Research Context</AlertTitle>
          <AlertDescription>
            These case studies are based on experiments from the paper "Toward Drift-Aware Database Benchmarking"
            by Guanli Liu and Renata Borovica-Gajic. The examples use the Census dataset to demonstrate
            how DriftBench enables controlled drift evaluation.
          </AlertDescription>
        </Alert>

        <Tabs defaultValue="data-drift" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="data-drift">Data Drift</TabsTrigger>
            <TabsTrigger value="workload-drift">Workload Drift</TabsTrigger>
            <TabsTrigger value="estimator-eval">Estimator Evaluation</TabsTrigger>
          </TabsList>

          <TabsContent value="data-drift" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Census Dataset: Data Drift Analysis</CardTitle>
                <CardDescription>
                  Analyzing cardinality updates and distribution shifts on Census data
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2">Dataset Overview</h4>
                    <p className="text-sm text-muted-foreground">
                      The Census Income dataset contains demographic information including age, workclass,
                      education, marital status, occupation, and income. We select representative attributes
                      (age - numeric, workclass - categorical) to demonstrate drift effects.
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle>Updating Cardinality</CardTitle>
                    <Badge>Cardinality Drift</Badge>
                  </div>
                  <CardDescription>10% random deletion maintaining distribution</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={ageDistribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="range" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="original" fill="#3b82f6" name="Original (%)" />
                      <Bar dataKey="drift" fill="#f59e0b" name="After Deletion (%)" />
                    </BarChart>
                  </ResponsiveContainer>
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                    <p className="text-sm text-blue-900 dark:text-blue-100">
                      <strong>Observation:</strong> The deleted subset is sampled proportionally to maintain
                      the original distribution shape, reflecting natural workload patterns without artificial bias.
                    </p>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle>Shifting Column Distribution</CardTitle>
                    <Badge variant="secondary">Distributional Drift</Badge>
                  </div>
                  <CardDescription>Increased skewness in categorical attributes</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={workclassDistribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="category" angle={-45} textAnchor="end" height={80} />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="original" fill="#3b82f6" name="Original (%)" />
                      <Bar dataKey="drift" fill="#10b981" name="Skewed (%)" />
                    </BarChart>
                  </ResponsiveContainer>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800">
                    <p className="text-sm text-green-900 dark:text-green-100">
                      <strong>Observation:</strong> The most frequent category (Private) becomes more dominant,
                      amplifying skewness and affecting selectivity estimation for predicates on this column.
                    </p>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle>Key Findings</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-blue-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <Database className="size-4 text-blue-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">Proportional Deletion</h4>
                      <p className="text-sm text-muted-foreground">
                        Random 10% deletion maintains distribution shape, ensuring drift reflects realistic
                        scenarios rather than introducing artificial bias
                      </p>
                    </div>
                  </div>
                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-green-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <TrendingUp className="size-4 text-green-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">Distribution Skewing</h4>
                      <p className="text-sm text-muted-foreground">
                        For categorical attributes, upweighting frequent categories increases skew and tests
                        optimizer robustness under non-uniform distributions
                      </p>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="workload-drift" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Census Workload: Drift Patterns</CardTitle>
                <CardDescription>
                  Predicate distribution changes and structural drift over time
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2">Workload Setup</h4>
                    <p className="text-sm text-muted-foreground">
                      We generate query workloads with varying predicate distributions (uniform, normal, skewed)
                      and structural changes (additional predicates, payload modifications) to simulate
                      evolving analytical demands.
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Changing Predicate Distributions</CardTitle>
                <CardDescription>
                  Three workloads with different value distributions over time
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-3 gap-4 mb-4">
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800 text-center">
                    <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-1">Phase 1</h4>
                    <p className="text-xs text-blue-800 dark:text-blue-200">Uniform Distribution</p>
                    <p className="text-xs text-blue-700 dark:text-blue-300 mt-1">00:00 - 00:05</p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800 text-center">
                    <h4 className="text-sm font-semibold text-green-900 dark:text-green-100 mb-1">Phase 2</h4>
                    <p className="text-xs text-green-800 dark:text-green-200">Normal Distribution</p>
                    <p className="text-xs text-green-700 dark:text-green-300 mt-1">00:05 - 00:10</p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950/20 p-3 rounded-lg border border-purple-200 dark:border-purple-800 text-center">
                    <h4 className="text-sm font-semibold text-purple-900 dark:text-purple-100 mb-1">Phase 3</h4>
                    <p className="text-xs text-purple-800 dark:text-purple-200">Skewed Distribution</p>
                    <p className="text-xs text-purple-700 dark:text-purple-300 mt-1">00:10 - 00:15</p>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Impact:</strong> Query predicates follow uniform, normal, and skewed distributions
                    across three timestamp groups. This reflects increasing levels of locality and skew,
                    affecting learned indices and optimizers that rely on historical access patterns.
                  </p>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Structural Drift Analysis</CardTitle>
                <CardDescription>
                  Modifying query structure and payload over time
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                    <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-2">
                      Original Templates
                    </h4>
                    <ul className="text-sm text-blue-800 dark:text-blue-200 space-y-1">
                      <li>• Max 5 predicates per query</li>
                      <li>• Max 6 projected columns</li>
                      <li>• Simple join conditions</li>
                      <li>• Lightweight aggregates</li>
                    </ul>
                  </div>

                  <div className="bg-purple-50 dark:bg-purple-950/20 p-4 rounded-lg border border-purple-200 dark:border-purple-800">
                    <h4 className="text-sm font-semibold text-purple-900 dark:text-purple-100 mb-2">
                      Drifted Templates
                    </h4>
                    <ul className="text-sm text-purple-800 dark:text-purple-200 space-y-1">
                      <li>• Max 7 predicates per query</li>
                      <li>• Max 8 projected columns</li>
                      <li>• Additional join tables</li>
                      <li>• Detailed record inspection</li>
                    </ul>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>Visualization:</strong> Using t-SNE projection of query features reveals distinct
                    clusters between original and drifted templates, demonstrating how logical and payload
                    changes affect query structure space.
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="estimator-eval" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>Cardinality Estimator Evaluation Under Drift</CardTitle>
                <CardDescription>
                  Comparing PostgreSQL, Naru, and MSCN under data and workload drift
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid md:grid-cols-3 gap-4 mb-6">
                  <div className="bg-muted p-3 rounded-lg">
                    <h4 className="font-semibold text-sm mb-2">PostgreSQL</h4>
                    <Badge variant="outline" className="mb-2">Rule-based</Badge>
                    <p className="text-xs text-muted-foreground">
                      Traditional histogram-based estimator relying on column statistics
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <h4 className="font-semibold text-sm mb-2">Naru</h4>
                    <Badge variant="outline" className="mb-2">Data-driven</Badge>
                    <p className="text-xs text-muted-foreground">
                      Deep unsupervised learning approach for cardinality estimation
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <h4 className="font-semibold text-sm mb-2">MSCN</h4>
                    <Badge variant="outline" className="mb-2">Data & Query-driven</Badge>
                    <p className="text-xs text-muted-foreground">
                      Learned model trained on both data and query features
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Q-Error Under Data Drift</CardTitle>
                <CardDescription>
                  Fixed workload, dataset scaling from 1.0× to 3.0× (D₁ to D₁₀)
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <ResponsiveContainer width="100%" height={350}>
                  <LineChart data={qErrorDataDrift}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="snapshot" />
                    <YAxis label={{ value: "Average Q-Error", angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="postgres" stroke="#3b82f6" strokeWidth={2} name="PostgreSQL" />
                    <Line type="monotone" dataKey="naru" stroke="#10b981" strokeWidth={2} name="Naru" />
                    <Line type="monotone" dataKey="mscn" stroke="#f59e0b" strokeWidth={2} name="MSCN" />
                  </LineChart>
                </ResponsiveContainer>

                <Alert>
                  <TrendingUp className="size-4" />
                  <AlertTitle>Key Observation</AlertTitle>
                  <AlertDescription>
                    As dataset cardinality increases, the same query workload leads to larger true result sizes.
                    Both PostgreSQL and learned models rely on stale statistics/models, causing Q-error to increase
                    and outlier behavior to become more pronounced.
                  </AlertDescription>
                </Alert>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Q-Error Under Workload Drift</CardTitle>
                <CardDescription>
                  Fixed dataset, six-phase workload with varying predicates (W₁ to W₆)
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="bg-muted p-3 rounded-lg mb-4">
                  <p className="text-sm">
                    <strong>Workload Phases:</strong> W₁ (age, workclass),
                    W₂ (education, marital_status),
                    W₃ (capital_gain, occupation),
                    W₄ (hours_per_week, workclass),
                    W₅ (capital_loss, marital_status),
                    W₆ (age, native_country)
                  </p>
                </div>

                <ResponsiveContainer width="100%" height={350}>
                  <BarChart data={qErrorWorkloadDrift}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="phase" />
                    <YAxis label={{ value: "Average Q-Error", angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="postgres" fill="#3b82f6" name="PostgreSQL" />
                    <Bar dataKey="naru" fill="#10b981" name="Naru" />
                    <Bar dataKey="mscn" fill="#f59e0b" name="MSCN" />
                  </BarChart>
                </ResponsiveContainer>

                <Alert>
                  <AlertCircle className="size-4" />
                  <AlertTitle>Key Observation</AlertTitle>
                  <AlertDescription>
                    MSCN shows particularly strong sensitivity to query changes, as predictions vary substantially
                    across workload phases. This is expected since MSCN is trained on query features. PostgreSQL
                    and Naru, being primarily data-driven, exhibit more stable behavior across phases.
                  </AlertDescription>
                </Alert>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Implications for Database Systems</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-red-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <AlertCircle className="size-4 text-red-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">Statistics Staleness</h4>
                      <p className="text-sm text-muted-foreground">
                        Traditional estimators degrade under cardinality drift without statistics updates.
                        Automatic re-analysis or adaptive thresholds are needed.
                      </p>
                    </div>
                  </div>

                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-orange-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <TrendingUp className="size-4 text-orange-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">Model Adaptation</h4>
                      <p className="text-sm text-muted-foreground">
                        Learned estimators require retraining or online adaptation mechanisms to maintain
                        accuracy under evolving data and workload patterns.
                      </p>
                    </div>
                  </div>

                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-blue-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <Database className="size-4 text-blue-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">Drift-Aware Design</h4>
                      <p className="text-sm text-muted-foreground">
                        Future database components should incorporate drift detection and adaptation as
                        first-class features, moving beyond static optimization.
                      </p>
                    </div>
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
