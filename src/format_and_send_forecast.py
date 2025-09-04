def build_email_content(forecast_data: dict) -> str:
    """
    Commit Note: Customer-optimized email template based on 2025 financial services research.
    Professional, clean design with human-readable scenario language.
    """
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ZeroDay Zen Forecast</title>
        <style>
            /* Mobile-first responsive design */
            body {{
                font-family: Arial, 'Helvetica Neue', Helvetica, sans-serif;
                background-color: #ffffff;
                color: #333333;
                margin: 0;
                padding: 20px;
                line-height: 1.6;
                font-size: 16px;
            }}
            
            .container {{
                max-width: 600px;
                margin: 0 auto;
                background-color: #ffffff;
                border: 1px solid #e5e5e5;
                border-radius: 8px;
                overflow: hidden;
            }}
            
            .header {{
                background-color: #f8f9fa;
                padding: 25px 20px;
                text-align: center;
                border-bottom: 3px solid #2c5282;
            }}
            
            .logo {{
                font-size: 24px;
                font-weight: bold;
                color: #2c5282;
                margin-bottom: 8px;
            }}
            
            .date-line {{
                font-size: 14px;
                color: #666666;
                margin-bottom: 5px;
            }}
            
            .tagline {{
                font-size: 12px;
                color: #888888;
                font-style: italic;
            }}
            
            .content {{
                padding: 20px;
            }}
            
            .section {{
                margin-bottom: 25px;
                padding-bottom: 20px;
                border-bottom: 1px solid #f0f0f0;
            }}
            
            .section:last-child {{
                border-bottom: none;
                margin-bottom: 0;
            }}
            
            .section-header {{
                font-size: 18px;
                font-weight: bold;
                color: #2c5282;
                margin-bottom: 12px;
                padding-bottom: 5px;
                border-bottom: 2px solid #e2e8f0;
            }}
            
            .market-data {{
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 6px;
                margin-bottom: 15px;
            }}
            
            .data-row {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 8px 0;
                border-bottom: 1px solid #e5e7eb;
            }}
            
            .data-row:last-child {{
                border-bottom: none;
            }}
            
            .data-label {{
                font-weight: bold;
                color: #374151;
            }}
            
            .data-value {{
                color: #1f2937;
                font-weight: 600;
            }}
            
            .forecast-box {{
                background-color: #eff6ff;
                border-left: 4px solid #3b82f6;
                padding: 20px;
                border-radius: 0 6px 6px 0;
                margin: 15px 0;
            }}
            
            .forecast-main {{
                font-size: 20px;
                font-weight: bold;
                color: #1e40af;
                margin-bottom: 8px;
                text-align: center;
            }}
            
            .levels-grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 15px;
                margin: 15px 0;
            }}
            
            .level-item {{
                text-align: center;
                padding: 12px;
                background-color: #f9fafb;
                border-radius: 6px;
                border: 1px solid #e5e7eb;
            }}
            
            .level-label {{
                font-size: 12px;
                color: #6b7280;
                margin-bottom: 4px;
                text-transform: uppercase;
                font-weight: 600;
            }}
            
            .level-value {{
                font-size: 16px;
                font-weight: bold;
            }}
            
            .support {{ color: #059669; }}
            .resistance {{ color: #dc2626; }}
            
            .scenario-list {{
                list-style: none;
                padding: 0;
                margin: 10px 0;
            }}
            
            .scenario-item {{
                padding: 8px 0;
                border-bottom: 1px solid #f3f4f6;
            }}
            
            .scenario-item:last-child {{
                border-bottom: none;
            }}
            
            .scenario-label {{
                font-weight: bold;
                color: #374151;
            }}
            
            .news-item {{
                background-color: #fefefe;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
                padding: 15px;
                margin: 10px 0;
            }}
            
            .disclaimer {{
                background-color: #fef3c7;
                border: 1px solid #f59e0b;
                padding: 15px;
                margin: 20px 0;
                border-radius: 6px;
                font-size: 12px;
                color: #92400e;
            }}
            
            .footer {{
                background-color: #f8f9fa;
                padding: 20px;
                text-align: center;
                font-size: 12px;
                color: #666666;
                border-top: 1px solid #e5e5e5;
            }}
            
            /* Mobile responsive adjustments */
            @media only screen and (max-width: 600px) {{
                .container {{
                    margin: 0 10px;
                    border-radius: 0;
                }}
                
                .levels-grid {{
                    grid-template-columns: 1fr;
                    gap: 10px;
                }}
                
                .data-row {{
                    flex-direction: column;
                    align-items: flex-start;
                    gap: 4px;
                }}
                
                .header {{
                    padding: 20px 15px;
                }}
                
                .content {{
                    padding: 15px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <!-- Header -->
            <div class="header">
                <div class="logo">ZeroDay Zen Forecast</div>
                <div class="date-line">{forecast_data.get("date", "")}</div>
                <div class="tagline">Professional Market Analysis</div>
            </div>
            
            <!-- Content -->
            <div class="content">
                <!-- Market Data Section -->
                <div class="section">
                    <div class="section-header">Market Data</div>
                    <div class="market-data">
                        <div class="data-row">
                            <span class="data-label">SPX</span>
                            <span class="data-value">{forecast_data.get("spx", "N/A")}</span>
                        </div>
                        <div class="data-row">
                            <span class="data-label">/ES</span>
                            <span class="data-value">{forecast_data.get("mes", "N/A")}</span>
                        </div>
                        <div class="data-row">
                            <span class="data-label">VIX</span>
                            <span class="data-value">{forecast_data.get("vix", "N/A")}</span>
                        </div>
                        <div class="data-row">
                            <span class="data-label">VVIX</span>
                            <span class="data-value">{forecast_data.get("vvix", "N/A")}</span>
                        </div>
                    </div>
                </div>
                
                <!-- Forecast Section -->
                <div class="section">
                    <div class="section-header">Market Forecast</div>
                    <div class="forecast-box">
                        <div class="forecast-main">{forecast_data.get("bias", "Neutral")}</div>
                    </div>
                </div>
                
                <!-- Key Levels Section -->
                <div class="section">
                    <div class="section-header">Key Levels</div>
                    <div class="levels-grid">
                        <div class="level-item">
                            <div class="level-label">Support</div>
                            <div class="level-value support">{forecast_data.get("support", "TBD")}</div>
                        </div>
                        <div class="level-item">
                            <div class="level-label">Resistance</div>
                            <div class="level-value resistance">{forecast_data.get("resistance", "TBD")}</div>
                        </div>
                    </div>
                </div>
                
                <!-- Scenarios Section -->
                <div class="section">
                    <div class="section-header">Probable Scenarios</div>
                    <ul class="scenario-list">
                        <li class="scenario-item">
                            <span class="scenario-label">Most Likely:</span> {forecast_data.get("base_case", "Expect sideways trading between key levels")}
                        </li>
                        <li class="scenario-item">
                            <span class="scenario-label">Bear Case:</span> {forecast_data.get("bear_case", "If we break below support then next likely target is lower")}
                        </li>
                        <li class="scenario-item">
                            <span class="scenario-label">Bull Case:</span> {forecast_data.get("bull_case", "If we break above resistance then next likely target is higher")}
                        </li>
                    </ul>
                </div>
                
                <!-- Market Context Section -->
                <div class="section">
                    <div class="section-header">Market Context</div>
                    <div class="news-item">
                        <strong>Current Environment:</strong> {forecast_data.get("news_context", "Monitoring key economic indicators and market sentiment.")}
                    </div>
                    <div style="margin-top: 10px; font-style: italic; color: #666;">
                        Analysis: {forecast_data.get("zen_analysis", "Standard market conditions observed.")}
                    </div>
                </div>
                
                <!-- Summary Section -->
                <div class="section">
                    <div class="section-header">Summary</div>
                    <p><strong>Outlook:</strong> {forecast_data.get("bias", "Neutral")} bias maintained. {forecast_data.get("notes", "Continue monitoring key levels and market developments.")}</p>
                </div>
                
                <!-- Educational Disclaimer -->
                <div class="disclaimer">
                    <strong>Educational Purpose Only:</strong> This forecast is provided for educational and informational purposes only. This is not investment advice or a recommendation to buy or sell securities. Market conditions can change rapidly. Always consult with a qualified financial advisor before making investment decisions.
                </div>
            </div>
            
            <!-- Footer -->
            <div class="footer">
                Generated by ZeroDay Zen Market Analysis<br>
                This report was generated automatically using proprietary algorithms.
            </div>
        </div>
    </body>
    </html>
    """
    return html_body