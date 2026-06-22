import {build} from "esbuild";

await build({
    entryPoints: ["src/index.ts"],
    bundle: true,
    outfile: "dist/lib/index.js",
    format: "esm",
    globalName: "Camp",
    platform: "browser",
    sourcemap: true,
    minify: true,
    legalComments: "inline",
    treeShaking: true,
    logLevel: "info",
});