import { Link } from "react-router";
import { ArrowRight, Database, TrendingUp, Wand2, Target } from "lucide-react";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";

export function Home() {
  return (
    <div className="container mx-auto px-4 py-12">
      <section className="text-center py-20">
        <div className="inline-flex items-center gap-2 px-4 py-2 bg-primary/10 rounded-full mb-6">
          <Database className="size-5 text-primary" />
          <span className="text-sm font-medium">Drift-Aware Database Benchmarking</span>
        </div>
        <h1 className="text-5xl font-bold mb-6 bg-gradient-to-r from-primary to-purple-600 bg-clip-text text-transparent">
          DriftBench
        </h1>
        <p className="text-xl text-muted-foreground max-w-3xl mx-auto mb-8">
          A unified foundation for drift-aware benchmarking, enabling evaluation of database systems
          under evolving data and workloads through DriftSpec specifications.
        </p>
        <div className="flex gap-4 justify-center flex-wrap">
          <Button asChild size="lg">
            <Link to="/drift-types">
              Explore Drift Types <ArrowRight className="ml-2 size-4" />
            </Link>
          </Button>
          <Button asChild variant="outline" size="lg">
            <Link to="/generator">
              Generate Drift <Wand2 className="ml-2 size-4" />
            </Link>
          </Button>
        </div>
      </section>

      <section className="py-16 grid md:grid-cols-3 gap-6">
        <Card>
          <CardHeader>
            <div className="size-12 rounded-lg bg-blue-100 dark:bg-blue-900/20 flex items-center justify-center mb-4">
              <Database className="size-6 text-blue-600 dark:text-blue-400" />
            </div>
            <CardTitle>Taxonomy of Drift</CardTitle>
            <CardDescription>
              Conceptualize data, workload, and temporal drift within a unified taxonomy
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>• Cardinality drift (scaling, updating)</li>
              <li>• Distribution drift (column shifts, outliers)</li>
              <li>• Workload drift (predicates, selectivity, structure)</li>
              <li>• Temporal patterns (uniform, periodic, trend)</li>
            </ul>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="size-12 rounded-lg bg-purple-100 dark:bg-purple-900/20 flex items-center justify-center mb-4">
              <Wand2 className="size-6 text-purple-600 dark:text-purple-400" />
            </div>
            <CardTitle>Drift Generator</CardTitle>
            <CardDescription>
              Interactive tool to upload datasets and generate custom drift scenarios
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>• Upload CSV or connect to database</li>
              <li>• Configure data and workload drift</li>
              <li>• Automatic DriftSpec generation</li>
              <li>• Download ready-to-use YAML files</li>
            </ul>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="size-12 rounded-lg bg-green-100 dark:bg-green-900/20 flex items-center justify-center mb-4">
              <Target className="size-6 text-green-600 dark:text-green-400" />
            </div>
            <CardTitle>Prototype Framework</CardTitle>
            <CardDescription>
              Modular framework implementing DriftSpec for synthesis and evaluation
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>• Schema extraction and analysis</li>
              <li>• Distribution simulation</li>
              <li>• Data and workload generation</li>
              <li>• Timestamp generation for temporal drift</li>
            </ul>
          </CardContent>
        </Card>
      </section>

      <section className="py-16">
        <h2 className="text-3xl font-bold text-center mb-12">Key Contributions</h2>
        <div className="grid md:grid-cols-2 gap-8 max-w-4xl mx-auto">
          <div className="flex gap-4">
            <div className="size-10 rounded-full bg-primary/10 flex items-center justify-center flex-shrink-0">
              <TrendingUp className="size-5 text-primary" />
            </div>
            <div>
              <h3 className="font-semibold mb-2">Making Drift a First-Class Concept</h3>
              <p className="text-sm text-muted-foreground">
                Shift benchmarking from static, one-off tests to controlled, continuous evaluation under drift
              </p>
            </div>
          </div>

          <div className="flex gap-4">
            <div className="size-10 rounded-full bg-primary/10 flex items-center justify-center flex-shrink-0">
              <Database className="size-5 text-primary" />
            </div>
            <div>
              <h3 className="font-semibold mb-2">Standardized Executable Language</h3>
              <p className="text-sm text-muted-foreground">
                Study how data and workload evolution influence database behavior through reproducible specifications
              </p>
            </div>
          </div>

          <div className="flex gap-4">
            <div className="size-10 rounded-full bg-primary/10 flex items-center justify-center flex-shrink-0">
              <Wand2 className="size-5 text-primary" />
            </div>
            <div>
              <h3 className="font-semibold mb-2">Benchmark Integration</h3>
              <p className="text-sm text-muted-foreground">
                Compatible with TPC-H, RedBench, and custom schemas for both benchmark-driven and trace-driven settings
              </p>
            </div>
          </div>

          <div className="flex gap-4">
            <div className="size-10 rounded-full bg-primary/10 flex items-center justify-center flex-shrink-0">
              <Target className="size-5 text-primary" />
            </div>
            <div>
              <h3 className="font-semibold mb-2">Practical Evaluation</h3>
              <p className="text-sm text-muted-foreground">
                Demonstrated through case studies on cardinality estimators and learned indices under drift
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="py-16 text-center">
        <Card className="max-w-3xl mx-auto bg-gradient-to-br from-primary/5 to-purple-600/5">
          <CardContent className="pt-6">
            <h2 className="text-2xl font-bold mb-4">Ready to Get Started?</h2>
            <p className="text-muted-foreground mb-6">
              Explore different drift types, generate your own DriftSpec configurations, and visualize
              how drift affects database components
            </p>
            <div className="flex gap-4 justify-center flex-wrap">
              <Button asChild>
                <Link to="/drift-types">Learn About Drift Types</Link>
              </Button>
              <Button asChild variant="outline">
                <Link to="/visualization">See Visualizations</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
