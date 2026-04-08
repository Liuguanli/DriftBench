import { Link } from "react-router";
import { ArrowRight, Database, TrendingUp, Wand2, Target } from "lucide-react";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { useI18n } from "../i18n";

export function Home() {
  const { t, messages } = useI18n();
  return (
    <div className="container mx-auto px-4 py-12">
      <section className="text-center py-20">
        <div className="inline-flex items-center gap-2 px-4 py-2 bg-primary/10 rounded-full mb-6">
          <Database className="size-5 text-primary" />
          <span className="text-sm font-medium">{t("home.tagline")}</span>
        </div>
        <h1 className="text-5xl font-bold mb-6 bg-gradient-to-r from-primary to-purple-600 bg-clip-text text-transparent">
          {t("home.title")}
        </h1>
        <p className="text-xl text-muted-foreground max-w-3xl mx-auto mb-8">
          {t("home.subtitle")}
        </p>
        <div className="flex gap-4 justify-center flex-wrap">
          <Button asChild size="lg">
            <Link to="/drift-types">
              {t("home.explore")} <ArrowRight className="ml-2 size-4" />
            </Link>
          </Button>
          <Button asChild variant="outline" size="lg">
            <Link to="/generator">
              {t("home.generate")} <Wand2 className="ml-2 size-4" />
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
            <CardTitle>{t("home.features.taxonomyTitle")}</CardTitle>
            <CardDescription>
              {t("home.features.taxonomyDesc")}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              {messages.home.features.taxonomyItems.map((item) => (
                <li key={item}>• {item}</li>
              ))}
            </ul>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="size-12 rounded-lg bg-purple-100 dark:bg-purple-900/20 flex items-center justify-center mb-4">
              <Wand2 className="size-6 text-purple-600 dark:text-purple-400" />
            </div>
            <CardTitle>{t("home.features.generatorTitle")}</CardTitle>
            <CardDescription>
              {t("home.features.generatorDesc")}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              {messages.home.features.generatorItems.map((item) => (
                <li key={item}>• {item}</li>
              ))}
            </ul>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="size-12 rounded-lg bg-green-100 dark:bg-green-900/20 flex items-center justify-center mb-4">
              <Target className="size-6 text-green-600 dark:text-green-400" />
            </div>
            <CardTitle>{t("home.features.frameworkTitle")}</CardTitle>
            <CardDescription>
              {t("home.features.frameworkDesc")}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-muted-foreground">
              {messages.home.features.frameworkItems.map((item) => (
                <li key={item}>• {item}</li>
              ))}
            </ul>
          </CardContent>
        </Card>
      </section>

      <section className="py-16">
        <h2 className="text-3xl font-bold text-center mb-12">{t("home.contributionsTitle")}</h2>
        <div className="grid md:grid-cols-2 gap-8 max-w-4xl mx-auto">
          {messages.home.contributions.map((item, index) => {
            const icons = [TrendingUp, Database, Wand2, Target];
            const Icon = icons[index] ?? TrendingUp;
            return (
              <div key={item.title} className="flex gap-4">
                <div className="size-10 rounded-full bg-primary/10 flex items-center justify-center flex-shrink-0">
                  <Icon className="size-5 text-primary" />
                </div>
                <div>
                  <h3 className="font-semibold mb-2">{item.title}</h3>
                  <p className="text-sm text-muted-foreground">{item.desc}</p>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      <section className="py-16 text-center">
        <Card className="max-w-3xl mx-auto bg-gradient-to-br from-primary/5 to-purple-600/5">
          <CardContent className="pt-6">
            <h2 className="text-2xl font-bold mb-4">{t("home.ctaTitle")}</h2>
            <p className="text-muted-foreground mb-6">{t("home.ctaDesc")}</p>
            <div className="flex gap-4 justify-center flex-wrap">
              <Button asChild>
                <Link to="/drift-types">{t("home.ctaPrimary")}</Link>
              </Button>
              <Button asChild variant="outline">
                <Link to="/visualization">{t("home.ctaSecondary")}</Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
