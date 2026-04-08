import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Badge } from "../components/ui/badge";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LineChart, Line } from "recharts";
import { AlertCircle, TrendingUp, Database } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import { useI18n } from "../i18n";

export function CaseStudies() {
  const { t, messages } = useI18n();
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
          <h1 className="text-4xl font-bold mb-4">{t("caseStudies.title")}</h1>
          <p className="text-lg text-muted-foreground">
            {t("caseStudies.subtitle")}
          </p>
        </div>

        <Alert className="mb-8">
          <AlertCircle className="size-4" />
          <AlertTitle>{t("caseStudies.researchTitle")}</AlertTitle>
          <AlertDescription>
            {t("caseStudies.researchBody")}
          </AlertDescription>
        </Alert>

        <Tabs defaultValue="data-drift" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="data-drift">{t("caseStudies.tabs.data")}</TabsTrigger>
            <TabsTrigger value="workload-drift">{t("caseStudies.tabs.workload")}</TabsTrigger>
            <TabsTrigger value="estimator-eval">{t("caseStudies.tabs.estimator")}</TabsTrigger>
          </TabsList>

          <TabsContent value="data-drift" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.data.title")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.data.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2">{t("caseStudies.data.overviewTitle")}</h4>
                    <p className="text-sm text-muted-foreground">
                      {t("caseStudies.data.overviewBody")}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle>{t("caseStudies.data.updatingTitle")}</CardTitle>
                    <Badge>{t("caseStudies.data.cardinalityBadge")}</Badge>
                  </div>
                  <CardDescription>{t("caseStudies.data.updatingDesc")}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={ageDistribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="range" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="original" fill="#3b82f6" name={t("caseStudies.labels.originalPct")} />
                      <Bar dataKey="drift" fill="#f59e0b" name={t("caseStudies.labels.afterDeletionPct")} />
                    </BarChart>
                  </ResponsiveContainer>
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                    <p className="text-sm text-blue-900 dark:text-blue-100">
                      <strong>{t("caseStudies.data.updatingObsTitle")}</strong> {t("caseStudies.data.updatingObsBody")}
                    </p>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle>{t("caseStudies.data.shiftingTitle")}</CardTitle>
                    <Badge variant="secondary">{t("caseStudies.data.distBadge")}</Badge>
                  </div>
                  <CardDescription>{t("caseStudies.data.shiftingDesc")}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={workclassDistribution}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="category" angle={-45} textAnchor="end" height={80} />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="original" fill="#3b82f6" name={t("caseStudies.labels.originalPct")} />
                      <Bar dataKey="drift" fill="#10b981" name={t("caseStudies.labels.skewedPct")} />
                    </BarChart>
                  </ResponsiveContainer>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800">
                    <p className="text-sm text-green-900 dark:text-green-100">
                      <strong>{t("caseStudies.data.shiftingObsTitle")}</strong> {t("caseStudies.data.shiftingObsBody")}
                    </p>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.data.keyFindings")}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-blue-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <Database className="size-4 text-blue-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">{t("caseStudies.data.finding1Title")}</h4>
                      <p className="text-sm text-muted-foreground">
                        {t("caseStudies.data.finding1Body")}
                      </p>
                    </div>
                  </div>
                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-green-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <TrendingUp className="size-4 text-green-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">{t("caseStudies.data.finding2Title")}</h4>
                      <p className="text-sm text-muted-foreground">
                        {t("caseStudies.data.finding2Body")}
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
                <CardTitle>{t("caseStudies.workload.title")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.workload.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-muted p-4 rounded-lg">
                    <h4 className="font-semibold mb-2">{t("caseStudies.workload.setupTitle")}</h4>
                    <p className="text-sm text-muted-foreground">
                      {t("caseStudies.workload.setupBody")}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.workload.changingTitle")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.workload.changingDesc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-3 gap-4 mb-4">
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800 text-center">
                    <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-1">{t("caseStudies.workload.phase1")}</h4>
                    <p className="text-xs text-blue-800 dark:text-blue-200">{t("caseStudies.workload.uniform")}</p>
                    <p className="text-xs text-blue-700 dark:text-blue-300 mt-1">00:00 - 00:05</p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800 text-center">
                    <h4 className="text-sm font-semibold text-green-900 dark:text-green-100 mb-1">{t("caseStudies.workload.phase2")}</h4>
                    <p className="text-xs text-green-800 dark:text-green-200">{t("caseStudies.workload.normal")}</p>
                    <p className="text-xs text-green-700 dark:text-green-300 mt-1">00:05 - 00:10</p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950/20 p-3 rounded-lg border border-purple-200 dark:border-purple-800 text-center">
                    <h4 className="text-sm font-semibold text-purple-900 dark:text-purple-100 mb-1">{t("caseStudies.workload.phase3")}</h4>
                    <p className="text-xs text-purple-800 dark:text-purple-200">{t("caseStudies.workload.skewed")}</p>
                    <p className="text-xs text-purple-700 dark:text-purple-300 mt-1">00:10 - 00:15</p>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>{t("caseStudies.workload.impactTitle")}</strong> {t("caseStudies.workload.impactBody")}
                  </p>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.workload.structuralTitle")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.workload.structuralDesc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                    <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-2">
                      {t("caseStudies.workload.originalTemplates")}
                    </h4>
                    <ul className="text-sm text-blue-800 dark:text-blue-200 space-y-1">
                      {messages.caseStudies.workload.originalItems.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>

                  <div className="bg-purple-50 dark:bg-purple-950/20 p-4 rounded-lg border border-purple-200 dark:border-purple-800">
                    <h4 className="text-sm font-semibold text-purple-900 dark:text-purple-100 mb-2">
                      {t("caseStudies.workload.driftedTemplates")}
                    </h4>
                    <ul className="text-sm text-purple-800 dark:text-purple-200 space-y-1">
                      {messages.caseStudies.workload.driftedItems.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </div>

                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm">
                    <strong>{t("caseStudies.workload.visualizationTitle")}</strong>{" "}
                    {t("caseStudies.workload.visualizationBody")}
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="estimator-eval" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.estimator.title")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.estimator.desc")}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid md:grid-cols-3 gap-4 mb-6">
                  <div className="bg-muted p-3 rounded-lg">
                    <h4 className="font-semibold text-sm mb-2">{t("caseStudies.estimator.postgres")}</h4>
                    <Badge variant="outline" className="mb-2">{t("caseStudies.estimator.ruleBased")}</Badge>
                    <p className="text-xs text-muted-foreground">
                      {t("caseStudies.estimator.postgresDesc")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <h4 className="font-semibold text-sm mb-2">{t("caseStudies.estimator.naru")}</h4>
                    <Badge variant="outline" className="mb-2">{t("caseStudies.estimator.dataDriven")}</Badge>
                    <p className="text-xs text-muted-foreground">
                      {t("caseStudies.estimator.naruDesc")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <h4 className="font-semibold text-sm mb-2">{t("caseStudies.estimator.mscn")}</h4>
                    <Badge variant="outline" className="mb-2">{t("caseStudies.estimator.dataQueryDriven")}</Badge>
                    <p className="text-xs text-muted-foreground">
                      {t("caseStudies.estimator.mscnDesc")}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.estimator.qErrorDataTitle")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.estimator.qErrorDataDesc")}
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
                    <Line type="monotone" dataKey="postgres" stroke="#3b82f6" strokeWidth={2} name={t("caseStudies.estimator.postgres")} />
                    <Line type="monotone" dataKey="naru" stroke="#10b981" strokeWidth={2} name={t("caseStudies.estimator.naru")} />
                    <Line type="monotone" dataKey="mscn" stroke="#f59e0b" strokeWidth={2} name={t("caseStudies.estimator.mscn")} />
                  </LineChart>
                </ResponsiveContainer>

                <Alert>
                  <TrendingUp className="size-4" />
                  <AlertTitle>{t("caseStudies.estimator.observationTitle")}</AlertTitle>
                  <AlertDescription>
                    {t("caseStudies.estimator.observationData")}
                  </AlertDescription>
                </Alert>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.estimator.qErrorWorkloadTitle")}</CardTitle>
                <CardDescription>
                  {t("caseStudies.estimator.qErrorWorkloadDesc")}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="bg-muted p-3 rounded-lg mb-4">
                  <p className="text-sm">
                    {t("caseStudies.estimator.workloadPhases")}
                  </p>
                </div>

                <ResponsiveContainer width="100%" height={350}>
                  <BarChart data={qErrorWorkloadDrift}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="phase" />
                    <YAxis label={{ value: "Average Q-Error", angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="postgres" fill="#3b82f6" name={t("caseStudies.estimator.postgres")} />
                    <Bar dataKey="naru" fill="#10b981" name={t("caseStudies.estimator.naru")} />
                    <Bar dataKey="mscn" fill="#f59e0b" name={t("caseStudies.estimator.mscn")} />
                  </BarChart>
                </ResponsiveContainer>

                <Alert>
                  <AlertCircle className="size-4" />
                  <AlertTitle>{t("caseStudies.estimator.observationTitle")}</AlertTitle>
                  <AlertDescription>
                    {t("caseStudies.estimator.observationWorkload")}
                  </AlertDescription>
                </Alert>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t("caseStudies.estimator.implicationsTitle")}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-red-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <AlertCircle className="size-4 text-red-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">{t("caseStudies.estimator.implication1Title")}</h4>
                      <p className="text-sm text-muted-foreground">
                        {t("caseStudies.estimator.implication1Body")}
                      </p>
                    </div>
                  </div>

                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-orange-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <TrendingUp className="size-4 text-orange-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">{t("caseStudies.estimator.implication2Title")}</h4>
                      <p className="text-sm text-muted-foreground">
                        {t("caseStudies.estimator.implication2Body")}
                      </p>
                    </div>
                  </div>

                  <div className="flex gap-3">
                    <div className="size-6 rounded-full bg-blue-500/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <Database className="size-4 text-blue-500" />
                    </div>
                    <div>
                      <h4 className="font-semibold text-sm mb-1">{t("caseStudies.estimator.implication3Title")}</h4>
                      <p className="text-sm text-muted-foreground">
                        {t("caseStudies.estimator.implication3Body")}
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
