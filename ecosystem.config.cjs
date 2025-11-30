module.exports = {
    apps: [{
        name: "substream-orchestrator",
        script: "index.ts",
        interpreter: "./node_modules/.bin/tsx", // <--- Change this
        // interpreter_args: "--esm",           // <--- DELETE this line (tsx doesn't need it)
        env: {
            NODE_ENV: "production",
        }
    }]
}