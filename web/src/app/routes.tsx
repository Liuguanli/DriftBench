import { createBrowserRouter } from "react-router";
import { Home } from "./pages/Home";
import { DriftTypes } from "./pages/DriftTypes";
import { DriftGenerator } from "./pages/DriftGenerator";
import { Visualization } from "./pages/Visualization";
import { CaseStudies } from "./pages/CaseStudies";
import { Layout } from "./components/Layout";

export const router = createBrowserRouter([
  {
    path: "/",
    Component: Layout,
    children: [
      { index: true, Component: Home },
      { path: "drift-types", Component: DriftTypes },
      { path: "generator", Component: DriftGenerator },
      { path: "visualization", Component: Visualization },
      { path: "case-studies", Component: CaseStudies },
    ],
  },
]);
