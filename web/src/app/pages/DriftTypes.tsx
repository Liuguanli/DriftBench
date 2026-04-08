import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Badge } from "../components/ui/badge";
import { Database, Activity, Clock } from "lucide-react";
import { useI18n } from "../i18n";

export function DriftTypes() {
  const { t, messages } = useI18n();
  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-5xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-4">{t("driftTypes.title")}</h1>
          <p className="text-lg text-muted-foreground">
            {t("driftTypes.subtitle")}
          </p>
        </div>

        <Tabs defaultValue="data" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="data" className="flex items-center gap-2">
              <Database className="size-4" />
              {t("driftTypes.tabs.data")}
            </TabsTrigger>
            <TabsTrigger value="workload" className="flex items-center gap-2">
              <Activity className="size-4" />
              {t("driftTypes.tabs.workload")}
            </TabsTrigger>
            <TabsTrigger value="temporal" className="flex items-center gap-2">
              <Clock className="size-4" />
              {t("driftTypes.tabs.temporal")}
            </TabsTrigger>
          </TabsList>

          <TabsContent value="data" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("driftTypes.data.introTitle")}</CardTitle>
                <CardDescription>
                  {t("driftTypes.data.introDesc")}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm font-mono">
                    {t("driftTypes.data.definition")}
                  </p>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.data.scalingTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.data.scalingDesc")}
                      </CardDescription>
                    </div>
                    <Badge>{t("driftTypes.data.cardinalityBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.data.scalingBody")}
                    </p>
                  </div>
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                    <p className="text-xs font-semibold mb-1 text-blue-900 dark:text-blue-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      {t("driftTypes.data.scalingExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.data.useCases")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.data.scalingUses.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.data.updatingTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.data.updatingDesc")}
                      </CardDescription>
                    </div>
                    <Badge>{t("driftTypes.data.cardinalityBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.data.updatingBody")}
                    </p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800">
                    <p className="text-xs font-semibold mb-1 text-green-900 dark:text-green-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-green-800 dark:text-green-200">
                      {t("driftTypes.data.updatingExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.data.useCases")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.data.updatingUses.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.data.shiftingTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.data.shiftingDesc")}
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">{t("driftTypes.data.distributionBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.data.shiftingBody")}
                    </p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950/20 p-3 rounded-lg border border-purple-200 dark:border-purple-800">
                    <p className="text-xs font-semibold mb-1 text-purple-900 dark:text-purple-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-purple-800 dark:text-purple-200">
                      {t("driftTypes.data.shiftingExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.data.useCases")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.data.shiftingUses.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.data.outliersTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.data.outliersDesc")}
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">{t("driftTypes.data.distributionBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.data.outliersBody")}
                    </p>
                  </div>
                  <div className="bg-orange-50 dark:bg-orange-950/20 p-3 rounded-lg border border-orange-200 dark:border-orange-800">
                    <p className="text-xs font-semibold mb-1 text-orange-900 dark:text-orange-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-orange-800 dark:text-orange-200">
                      {t("driftTypes.data.outliersExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.data.useCases")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.data.outliersUses.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="workload" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("driftTypes.workload.introTitle")}</CardTitle>
                <CardDescription>
                  {t("driftTypes.workload.introDesc")}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted p-4 rounded-lg">
                  <p className="text-sm font-mono">
                    {t("driftTypes.workload.definition")}
                  </p>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.workload.predicateTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.workload.predicateDesc")}
                      </CardDescription>
                    </div>
                    <Badge>{t("driftTypes.workload.parametricBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.workload.predicateBody")}
                    </p>
                  </div>
                  <div className="bg-blue-50 dark:bg-blue-950/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                    <p className="text-xs font-semibold mb-1 text-blue-900 dark:text-blue-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      {t("driftTypes.workload.predicateExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.workload.impactLabel")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.workload.predicateImpact.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.workload.selectivityTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.workload.selectivityDesc")}
                      </CardDescription>
                    </div>
                    <Badge>{t("driftTypes.workload.parametricBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.workload.selectivityBody")}
                    </p>
                  </div>
                  <div className="bg-green-50 dark:bg-green-950/20 p-3 rounded-lg border border-green-200 dark:border-green-800">
                    <p className="text-xs font-semibold mb-1 text-green-900 dark:text-green-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-green-800 dark:text-green-200">
                      {t("driftTypes.workload.selectivityExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.workload.impactLabel")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.workload.selectivityImpact.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.workload.structureTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.workload.structureDesc")}
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">{t("driftTypes.workload.structuralBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.workload.structureBody")}
                    </p>
                  </div>
                  <div className="bg-purple-50 dark:bg-purple-950/20 p-3 rounded-lg border border-purple-200 dark:border-purple-800">
                    <p className="text-xs font-semibold mb-1 text-purple-900 dark:text-purple-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-purple-800 dark:text-purple-200">
                      {t("driftTypes.workload.structureExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.workload.impactLabel")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.workload.structureImpact.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <div className="flex items-start justify-between">
                    <div>
                      <CardTitle>{t("driftTypes.workload.payloadTitle")}</CardTitle>
                      <CardDescription className="mt-2">
                        {t("driftTypes.workload.payloadDesc")}
                      </CardDescription>
                    </div>
                    <Badge variant="secondary">{t("driftTypes.workload.structuralBadge")}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <p className="text-sm mb-3">
                      {t("driftTypes.workload.payloadBody")}
                    </p>
                  </div>
                  <div className="bg-orange-50 dark:bg-orange-950/20 p-3 rounded-lg border border-orange-200 dark:border-orange-800">
                    <p className="text-xs font-semibold mb-1 text-orange-900 dark:text-orange-100">
                      {t("driftTypes.data.exampleLabel")}
                    </p>
                    <p className="text-sm text-orange-800 dark:text-orange-200">
                      {t("driftTypes.workload.payloadExample")}
                    </p>
                  </div>
                  <div className="bg-muted p-3 rounded-lg">
                    <p className="text-xs font-semibold mb-1">{t("driftTypes.workload.impactLabel")}</p>
                    <ul className="text-sm space-y-1">
                      {messages.driftTypes.workload.payloadImpact.map((item) => (
                        <li key={item}>• {item}</li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="temporal" className="space-y-6 mt-6">
            <Card>
              <CardHeader>
                <CardTitle>{t("driftTypes.temporal.introTitle")}</CardTitle>
                <CardDescription>
                  {t("driftTypes.temporal.introDesc")}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted p-4 rounded-lg mb-4">
                  <p className="text-sm">
                    {t("driftTypes.temporal.introBody")}
                  </p>
                </div>
              </CardContent>
            </Card>

            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle>{t("driftTypes.temporal.uniformTitle")}</CardTitle>
                  <CardDescription>{t("driftTypes.temporal.uniformDesc")}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    {t("driftTypes.temporal.uniformBody")}
                  </p>
                  <div className="h-24 bg-gradient-to-r from-blue-100 to-blue-100 dark:from-blue-950 dark:to-blue-950 rounded-lg flex items-center justify-center">
                    <div className="w-full h-1 bg-blue-500"></div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>{t("driftTypes.temporal.periodicTitle")}</CardTitle>
                  <CardDescription>{t("driftTypes.temporal.periodicDesc")}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    {t("driftTypes.temporal.periodicBody")}
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
                  <CardTitle>{t("driftTypes.temporal.trendTitle")}</CardTitle>
                  <CardDescription>{t("driftTypes.temporal.trendDesc")}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    {t("driftTypes.temporal.trendBody")}
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
                  <CardTitle>{t("driftTypes.temporal.tailTitle")}</CardTitle>
                  <CardDescription>{t("driftTypes.temporal.tailDesc")}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-3">
                  <p className="text-sm">
                    {t("driftTypes.temporal.tailBody")}
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
                <CardTitle>{t("driftTypes.temporal.combineTitle")}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3 text-sm">
                  <p>
                    {t("driftTypes.temporal.combineBody")}
                  </p>
                  <ul className="space-y-2 ml-4">
                    {messages.driftTypes.temporal.combineItems.map((item) => (
                      <li key={item}>{item}</li>
                    ))}
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
