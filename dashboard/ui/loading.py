"""
Loading overlay utilities for the Retail Analytics Dashboard.
"""

import streamlit as st


def show_loading_overlay(
    message: str = "Syncing data...",
    submessage: str = "New data detected in cloud"
) -> None:
    """
    Show a fullscreen loading overlay with progress animation.

    Args:
        message: Main loading message
        submessage: Secondary description text
    """
    overlay_id = "retail-loading-overlay"

    overlay_html = f"""
    <script>
    (function() {{
        const existing = document.getElementById('{overlay_id}');
        if (existing) existing.remove();

        const style = document.createElement('style');
        style.id = '{overlay_id}-styles';
        style.textContent = `
            @keyframes retail-pulse {{
                0%, 100% {{ opacity: 1; }}
                50% {{ opacity: 0.5; }}
            }}
            @keyframes retail-progress {{
                0% {{ width: 0%; }}
                50% {{ width: 70%; }}
                100% {{ width: 100%; }}
            }}
            @keyframes retail-spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
        `;
        document.head.appendChild(style);

        const overlay = document.createElement('div');
        overlay.id = '{overlay_id}';
        overlay.innerHTML = `
            <div style="
                width: 60px;
                height: 60px;
                border: 4px solid rgba(255, 255, 255, 0.1);
                border-top: 4px solid #4CAF50;
                border-radius: 50%;
                animation: retail-spin 1s linear infinite;
                margin-bottom: 24px;
            "></div>
            <div style="
                color: white;
                font-size: 24px;
                font-weight: 600;
                margin-bottom: 8px;
                animation: retail-pulse 2s ease-in-out infinite;
            ">{message}</div>
            <div style="
                color: rgba(255, 255, 255, 0.7);
                font-size: 14px;
                margin-bottom: 32px;
            ">{submessage}</div>
            <div style="
                width: 300px;
                height: 6px;
                background: rgba(255, 255, 255, 0.1);
                border-radius: 3px;
                overflow: hidden;
            ">
                <div style="
                    height: 100%;
                    background: linear-gradient(90deg, #4CAF50, #8BC34A);
                    border-radius: 3px;
                    animation: retail-progress 3s ease-in-out infinite;
                "></div>
            </div>
            <div style="
                color: rgba(255, 255, 255, 0.5);
                font-size: 12px;
                margin-top: 16px;
            ">Please wait while we fetch the latest data...</div>
        `;

        Object.assign(overlay.style, {{
            position: 'fixed',
            top: '0',
            left: '0',
            width: '100vw',
            height: '100vh',
            background: 'rgba(0, 0, 0, 0.85)',
            backdropFilter: 'blur(8px)',
            webkitBackdropFilter: 'blur(8px)',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            zIndex: '999999',
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
        }});

        document.body.appendChild(overlay);

        setTimeout(function() {{
            const el = document.getElementById('{overlay_id}');
            if (el) {{
                el.style.transition = 'opacity 0.5s';
                el.style.opacity = '0';
                setTimeout(() => el.remove(), 500);
            }}
            const styleEl = document.getElementById('{overlay_id}-styles');
            if (styleEl) styleEl.remove();
        }}, 15000);
    }})();
    </script>
    """

    st.markdown(overlay_html, unsafe_allow_html=True)


def hide_loading_overlay() -> None:
    """Hide the loading overlay."""
    overlay_id = "retail-loading-overlay"
    hide_script = f"""
    <script>
    (function() {{
        const overlay = document.getElementById('{overlay_id}');
        if (overlay) {{
            overlay.style.transition = 'opacity 0.3s';
            overlay.style.opacity = '0';
            setTimeout(() => overlay.remove(), 300);
        }}
        const style = document.getElementById('{overlay_id}-styles');
        if (style) style.remove();
    }})();
    </script>
    """
    st.markdown(hide_script, unsafe_allow_html=True)
