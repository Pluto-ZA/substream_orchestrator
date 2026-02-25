module.exports = {
    apps: [{
        name: "wallet-balances",
        script: "./build/index.js",
        env: {
            NODE_ENV: "production",
        }
    }]
}