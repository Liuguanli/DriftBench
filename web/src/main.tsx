import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./app/App";
import { LanguageProvider } from "./app/i18n";
import "./styles/index.css";

const container = document.getElementById("root");

if (!container) {
  throw new Error("Root container missing in index.html");
}

createRoot(container).render(
  <React.StrictMode>
    <LanguageProvider>
      <App />
    </LanguageProvider>
  </React.StrictMode>,
);
