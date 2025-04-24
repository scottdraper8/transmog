// Custom JavaScript for Transmog documentation

document.addEventListener('DOMContentLoaded', function () {
    // Add copy buttons to all code blocks that don't already have them
    // (this is a fallback in case sphinx-copybutton doesn't work)
    if (typeof window.copyCellContent !== 'function') {
        document.querySelectorAll('div.highlight pre').forEach(function (block) {
            // Only add button if one doesn't exist already
            if (!block.parentNode.querySelector('.copybutton')) {
                var button = document.createElement('button');
                button.className = 'copybutton';
                button.textContent = 'Copy';

                button.addEventListener('click', function () {
                    var text = block.textContent;
                    navigator.clipboard.writeText(text).then(function () {
                        button.textContent = 'Copied!';
                        setTimeout(function () {
                            button.textContent = 'Copy';
                        }, 2000);
                    });
                });

                block.parentNode.insertBefore(button, block);
            }
        });
    }

    // Make external links open in a new tab
    document.querySelectorAll('a[href^="http"]').forEach(function (link) {
        if (!link.target) {
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
        }
    });

    // Add version selector behavior if present
    var versionSelector = document.getElementById('version-selector');
    if (versionSelector) {
        versionSelector.addEventListener('change', function () {
            window.location.href = this.value;
        });
    }

    // Initialize any tabbed content
    document.querySelectorAll('.tabbed-set').forEach(function (tabContainer) {
        var tabs = tabContainer.querySelectorAll('.tabbed-set > input');
        tabs.forEach(function (tab) {
            tab.addEventListener('change', function () {
                localStorage.setItem('selectedTab', this.id);
            });
        });

        // Restore previously selected tab
        var selectedTab = localStorage.getItem('selectedTab');
        if (selectedTab) {
            var tab = document.getElementById(selectedTab);
            if (tab && tabContainer.contains(tab)) {
                tab.checked = true;
            }
        }
    });
}); 