module.exports = {
    apps: [{
        name: "substream-orchestrator",
        script: "index.ts",
        // Use the local ts-node installed in your project
        interpreter: "./node_modules/.bin/ts-node",
        // vitally important for "type": "module" projects
        interpreter_args: "--esm",
        env: {
            NODE_ENV: "development",
        }
    }]
}