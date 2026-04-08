import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Badge } from "../components/ui/badge";
import { Database, Activity, Clock } from "lucide-react";

export function DriftTypes() {
  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-5xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-4">Drift Taxonomy</h1>
          <p className="text-lg text-muted-foreground">
            A working taxonomy of data and workload drift that captures recurring patterns observed
            across prior systems and benchmarks.
          </p>
        </div>

        <Tabs defaultValue="data" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="data" className="flex items-center gap-2">
              <Database className="size-4" />
              Data Drift
            </TabsTrigger>
            <TabsTrigger value="workload" className="flex items-center gap-2">
              <Activity className="size-4" />
              Workload Drift
            </TabsTrigger>
            <TabsTrigger value="temporal" className="flex items-center gap-2">
              <Clock className="size-4" />
              Temporal Drift
            </TabsTrigger>
          </TabsList>

          <TabsContent value="data" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>What is Data Drift?</CardTitle>
                <CardDescription>
                  Data drift refers to changes in the cardinality or distribution of records within a database.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm font-mono">
                    <strong>Definition:</strong> Let D₁ and D₂ denote two versions of a dataset over the same schema S.
                    Data drift is a significant change in the statistical properties or volume of data between D₁ and D₂.
                  </p>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Scaling Cardinality</CardTitle>
                      <CardDescription className="mt-2">
                        Changes in the overall number of records
                      </CardDescription>
                    </div>
                    <Badge>Cardinality Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Models net size effects under a new snapshot (e.g., 0.5×, 2×, 10×) without prescribing
                      how tuples arrived or departed.
                    </p>
                  </div>
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                    <p className="text-xs font-semibold mb-1 text-blue-900 dark:text-blue-100">Example</p>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      Population records gradually increase as coverage expands to new regions or higher birth rates
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Use Cases</p>
                    <ul className="text-sm space-y-1">
                      <li>• Stress storage footprint</li>
                      <li>• Evaluate plan costs under larger snapshots</li>
                      <li>• Test system scalability (TPC-H, TPC-DS)</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Updating Cardinality</CardTitle>
                      <CardDescription className="mt-2">
                        Time-ordered stream of inserts and deletes
                      </CardDescription>
                    </div>
                    <Badge>Cardinality Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Dataset is mutated over time rather than resized in one shot. Applies continuous
                      insertions and deletions.
                    </p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800">
                    <p className="text-xs font-semibold mb-1 text-green-900 dark:text-green-100">Example</p>
                    <p className="text-sm text-green-800 dark:text-green-200">
                      Individuals continuously added and removed due to births, deaths, and migration
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Use Cases</p>
                    <ul className="text-sm space-y-1">
                      <li>• Index maintenance evaluation</li>
                      <li>• Statistics freshness testing</li>
                      <li>• Continuous-update behavior analysis</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Shifting Column Distributions</CardTitle>
                      <CardDescription className="mt-2">
                        Changes in column value distributions
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">Distributional Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Captures changes such as increased skewness without altering dataset cardinality.
                      Affects spatial and temporal workloads.
                    </p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950/20 p-3 rounded-lg border border-purple-200 dark:border-purple-800">
                    <p className="text-xs font-semibold mb-1 text-purple-900 dark:text-purple-100">Example</p>
                    <p className="text-sm text-purple-800 dark:text-purple-200">
                      Student concentration during enrollment period shifts distribution of education-related attributes
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Use Cases</p>
                    <ul className="text-sm space-y-1">
                      <li>• Spatial workload evaluation</li>
                      <li>• Selectivity estimation testing</li>
                      <li>• Distribution-aware index performance</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Injecting Outliers</CardTitle>
                      <CardDescription className="mt-2">
                        Rare or extreme values for robustness testing
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">Distributional Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Uses rare or extreme values to test system robustness under distributional anomalies
                      and potential data poisoning.
                    </p>
                  </div>
                  <div className="bg-orange-50 dark:bg-orange-950/20 p-3 rounded-lg border border-orange-200 dark:border-orange-800">
                    <p className="text-xs font-semibold mb-1 text-orange-900 dark:text-orange-100">Example</p>
                    <p className="text-sm text-orange-800 dark:text-orange-200">
                      Small number of records with extremely large household sizes or unusually high incomes
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Use Cases</p>
                    <ul className="text-sm space-y-1">
                      <li>• Optimizer robustness testing</li>
                      <li>• Learned index data poisoning studies</li>
                      <li>• Statistics distortion evaluation</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="workload" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>What is Workload Drift?</CardTitle>
                <CardDescription>
                  Workload drift refers to changes in the structure or statistical properties of queries
                  executed against a database over time.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm font-mono">
                    <strong>Definition:</strong> A workload W is defined as a distribution P(Wτ(θ)) over queries
                    instantiated from a parameterized template τ, where θ ∈ Θ denotes the parameter-generating operator.
                  </p>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Changing Predicate Distributions</CardTitle>
                      <CardDescription className="mt-2">
                        Statistical distribution of predicates over time
                      </CardDescription>
                    </div>
                    <Badge>Parametric Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Drifts in the statistical distribution of predicates. Affects query optimizers and
                      learned indices that rely on historical access patterns.
                    </p>
                  </div>
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                    <p className="text-xs font-semibold mb-1 text-blue-900 dark:text-blue-100">Example</p>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      Census queries increasingly concentrate on major cities rather than evenly across all regions
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Impact</p>
                    <ul className="text-sm space-y-1">
                      <li>• Query optimizer accuracy</li>
                      <li>• Learned index effectiveness</li>
                      <li>• Access pattern optimization</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Varying Selectivity</CardTitle>
                      <CardDescription className="mt-2">
                        Queries from same template with varying ranges
                      </CardDescription>
                    </div>
                    <Badge>Parametric Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Queries from the same logical template exhibit varying predicate ranges, leading
                      to different join strategies or plan choices.
                    </p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800">
                    <p className="text-xs font-semibold mb-1 text-green-900 dark:text-green-100">Example</p>
                    <p className="text-sm text-green-800 dark:text-green-200">
                      Census workload repeatedly uses same query template while expanding predicate (age) ranges
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Impact</p>
                    <ul className="text-sm space-y-1">
                      <li>• Plan stability</li>
                      <li>• Join strategy selection</li>
                      <li>• Cardinality estimation accuracy</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Modifying Query Structure</CardTitle>
                      <CardDescription className="mt-2">
                        Changes in query templates and conditions
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">Structural Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Changes in query templates, such as modified predicates or join conditions.
                      Can trigger re-optimization or impact index usage.
                    </p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950/20 p-3 rounded-lg border border-purple-200 dark:border-purple-800">
                    <p className="text-xs font-semibold mb-1 text-purple-900 dark:text-purple-100">Example</p>
                    <p className="text-sm text-purple-800 dark:text-purple-200">
                      Census queries extended with additional joins or predicates (household, employment tables)
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Impact</p>
                    <ul className="text-sm space-y-1">
                      <li>• Re-optimization requirements</li>
                      <li>• Index selection changes</li>
                      <li>• Query plan structure</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>Changing Payloads</CardTitle>
                      <CardDescription className="mt-2">
                        Changes in projected column sets
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">Structural Drift</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      Changes in the set of projected columns. Distinct impact on I/O cost and
                      column scan behavior compared to other structural changes.
                    </p>
                  </div>
                  <div className="bg-orange-50 dark:bg-orange-950/20 p-3 rounded-lg border border-orange-200 dark:border-orange-800">
                    <p className="text-xs font-semibold mb-1 text-orange-900 dark:text-orange-100">Example</p>
                    <p className="text-sm text-orange-800 dark:text-orange-200">
                      Workload evolves from lightweight aggregates to detailed record inspection
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">Impact</p>
                    <ul className="text-sm space-y-1">
                      <li>• I/O cost variations</li>
                      <li>• Column scan behavior</li>
                      <li>• Output schema definition</li>
                    </ul>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="temporal" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>What is Temporal Drift?</CardTitle>
                <CardDescription>
                  Temporal drift models the evolution of data or workloads over time, following non-stationary
                  patterns such as bursts, trends, or repeats.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted p-4 rounded-lg mb-4">
                  <p className="text-sm">
                    We adopt the classification from <strong>Sibyl</strong>, which defines four representative
                    temporal patterns. These can be applied independently or in combination with data and workload drift.
                  </p>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle>Uniform Pattern</CardTitle>
                  <CardDescription>Constant rate over time</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    Events or queries arrive at a constant rate with uniform distribution across the time window.
                    Provides a baseline for comparison with other patterns.
                  </p>
                  <div className="h-24 bg-gradient-to-r from-blue-100 to-blue-100 dark:from-blue-950 dark:to-blue-950 rounded-lg flex items-center justify-center">
                    <div className="w-full h-1 bg-blue-500"></div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Periodic Pattern</CardTitle>
                  <CardDescription>Repeating cycles</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    Events follow repeating cycles with regular intervals. Common in real-world scenarios
                    with daily, weekly, or seasonal patterns.
                  </p>
                  <div className="h-24 bg-gradient-to-r from-green-100 to-green-100 dark:from-green-950 dark:to-green-950 rounded-lg relative overflow-hidden">
                    <svg viewBox="0 0 200 50" className="w-full h-full">
                      <path
                        d="M0,25 Q10,10 20,25 T40,25 T60,25 T80,25 T100,25 T120,25 T140,25 T160,25 T180,25 T200,25"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        className="text-green-500"
                      />
                    </svg>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Trend Pattern</CardTitle>
                  <CardDescription>Gradual increase or decrease</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    Events show a gradual increasing or decreasing trend over time. Reflects long-term
                    growth or decline in system usage.
                  </p>
                  <div className="h-24 bg-gradient-to-r from-purple-100 to-purple-100 dark:from-purple-950 dark:to-purple-950 rounded-lg relative overflow-hidden">
                    <svg viewBox="0 0 200 50" className="w-full h-full">
                      <path
                        d="M0,45 L200,5"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        className="text-purple-500"
                      />
                    </svg>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Long-tail Pattern</CardTitle>
                  <CardDescription>Burst followed by decay</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    High initial activity followed by exponential decay. Common in event-driven scenarios
                    and viral content access patterns.
                  </p>
                  <div className="h-24 bg-gradient-to-r from-orange-100 to-orange-100 dark:from-orange-950 dark:to-orange-950 rounded-lg relative overflow-hidden">
                    <svg viewBox="0 0 200 50" className="w-full h-full">
                      <path
                        d="M0,5 Q30,5 60,15 Q90,25 120,32 Q150,38 180,42 L200,45"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        className="text-orange-500"
                      />
                    </svg>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle>Combining Temporal with Data/Workload Drift</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3 text-sm">
                  <p>
                    Temporal patterns can be applied independently or combined with data and workload drift
                    to create more expressive scenarios:
                  </p>
                  <ul className="space-y-2 ml-4">
                    <li>
                      <strong>Query timestamps:</strong> Generated per instance to simulate realistic query
                      streams with temporal variation
                    </li>
                    <li>
                      <strong>Repetitive queries:</strong> Common in real-world cases, can be assigned periodic
                      timestamps
                    </li>
                    <li>
                      <strong>Data updates:</strong> Insertions and deletions can follow scheduled intervals
                      to reproduce read-heavy or write-heavy workloads
                    </li>
                    <li>
                      <strong>Cross-effects:</strong> Reveal performance degradation under bursty arrivals or
                      delayed adaptation to trends
                    </li>
                  </ul>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
