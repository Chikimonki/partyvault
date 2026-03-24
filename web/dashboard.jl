using HTTP, JSON, Printf

function serve()
    # Wait for output file to exist (simple loop)
    while !isfile("/app/output/quality_report.json")
        sleep(1)
    end

    # Read report
    report = JSON.parsefile("/app/output/quality_report.json")
    quality_score = report["quality_score"]
    anomalies = report["anomalies"]["details"]

    html = """
    <!DOCTYPE html>
    <html>
    <head><title>PartyVault Dashboard</title>
    <style>
        body { font-family: sans-serif; margin: 40px; background: #f5f5f5; }
        .score { font-size: 48px; font-weight: bold; margin: 20px 0; color: #333; }
        .anomalies { list-style: none; padding: 0; }
        .anomalies li { background: white; margin: 5px 0; padding: 10px; border-left: 4px solid #f44336; }
    </style>
    </head>
    <body>
        <h1>PartyVault Data Quality Dashboard</h1>
        <div class="score">Quality Score: $(round(quality_score, digits=1)) / 100</div>
        <h2>Anomalies ($(length(anomalies)))</h2>
        <ul class="anomalies">
        $(join(["<li><strong>$(a["party_id"])</strong>: $(a["description"])</li>" for a in anomalies], ""))
        </ul>
    </body>
    </html>
    """

    HTTP.serve(req -> HTTP.Response(200, html), "0.0.0.0", 8080)
end

serve()
