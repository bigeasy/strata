const count = 100000000

async function main () {
    {
        function one () {
            return two()
        }
        function two () {
            return 1
        }
        const start = Date.now()
        for (let i = 0; i < count; i++) {
            one()
        }
        console.log(Date.now() - start)
    }
    {
        async function one () {
            return await two()
        }
        async function two () {
            return 1
        }
        const start = Date.now()
        for (let i = 0; i < count; i++) {
            await one()
        }
        console.log(Date.now() - start)
    }
}

main()
