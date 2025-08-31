// LoadReady ETL Pipeline - Frontend JavaScript

document.addEventListener('DOMContentLoaded', function() {
    const fileInput = document.getElementById('fileInput');
    const fileUploadArea = document.getElementById('fileUploadArea');
    const fileList = document.getElementById('fileList');
    const browseBtn = document.getElementById('browseBtn');
    const processBtn = document.getElementById('processBtn');
    const uploadForm = document.getElementById('uploadForm');
    
    let selectedFiles = [];

    // File upload area click handler
    fileUploadArea.addEventListener('click', function() {
        fileInput.click();
    });

    // Browse button click handler
    browseBtn.addEventListener('click', function(e) {
        e.preventDefault();
        fileInput.click();
    });

    // Drag and drop handlers
    fileUploadArea.addEventListener('dragover', function(e) {
        e.preventDefault();
        fileUploadArea.classList.add('dragover');
    });

    fileUploadArea.addEventListener('dragleave', function(e) {
        e.preventDefault();
        fileUploadArea.classList.remove('dragover');
    });

    fileUploadArea.addEventListener('drop', function(e) {
        e.preventDefault();
        fileUploadArea.classList.remove('dragover');
        
        const files = Array.from(e.dataTransfer.files);
        handleFiles(files);
    });

    // File input change handler
    fileInput.addEventListener('change', function(e) {
        const files = Array.from(e.target.files);
        console.log('Files selected:', files.length);
        handleFiles(files);
    });

    // Form submit handler
    uploadForm.addEventListener('submit', function(e) {
        e.preventDefault();
        processFiles();
    });

    function handleFiles(files) {
        console.log('Handling files:', files.length);
        
        // Filter for CSV files only
        const csvFiles = files.filter(file => file.name.toLowerCase().endsWith('.csv'));
        
        if (csvFiles.length !== files.length) {
            alert('Only CSV files are allowed. Some files were ignored.');
        }
        
        if (csvFiles.length === 0) {
            alert('Please select at least one CSV file.');
            return;
        }
        
        selectedFiles = csvFiles;
        console.log('CSV files selected:', selectedFiles.length);
        displayFileList();
        updateProcessButton();
    }

    function displayFileList() {
        fileList.innerHTML = '';
        
        selectedFiles.forEach((file, index) => {
            const fileItem = document.createElement('div');
            fileItem.className = 'file-item';
            
            fileItem.innerHTML = `
                <div>
                    <span class="file-name">${file.name}</span>
                    <span class="file-size">(${formatFileSize(file.size)})</span>
                </div>
                <button type="button" class="remove-file" onclick="removeFile(${index})">Remove</button>
            `;
            
            fileList.appendChild(fileItem);
        });
    }

    function removeFile(index) {
        selectedFiles.splice(index, 1);
        displayFileList();
        updateProcessButton();
    }

    function updateProcessButton() {
        const hasFiles = selectedFiles.length > 0;
        processBtn.disabled = !hasFiles;
        
        if (hasFiles) {
            processBtn.textContent = `Process ${selectedFiles.length} File${selectedFiles.length > 1 ? 's' : ''}`;
            processBtn.classList.remove('btn-disabled');
        } else {
            processBtn.textContent = 'Process Files';
            processBtn.classList.add('btn-disabled');
        }
        
        console.log(`Files selected: ${selectedFiles.length}, Button disabled: ${processBtn.disabled}`);
    }

    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    async function processFiles() {
        try {
            // Show loading state
            showSection('loading');
            
            // Prepare form data
            const formData = new FormData();
            selectedFiles.forEach(file => {
                formData.append('files', file);
            });
            
            // Send files to backend
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });
            
            const result = await response.json();
            
            if (result.success) {
                displayResults(result);
                showSection('results');
            } else {
                showError(result.error);
            }
            
        } catch (error) {
            showError(`Network error: ${error.message}`);
        }
    }

    function displayResults(result) {
        const { summary, clean_data, quarantine_data } = result.results;
        
        // Update summary cards
        document.getElementById('totalProcessed').textContent = summary.total_processed;
        document.getElementById('cleanRecords').textContent = summary.clean_records;
        document.getElementById('quarantinedRecords').textContent = summary.quarantined_records;
        document.getElementById('successRate').textContent = summary.success_rate + '%';
        
        // Display clean data table
        displayDataTable('cleanDataTable', clean_data, [
            'id', 'name', 'age', 'salary', 'department', 'yearly_salary'
        ]);
        
        // Display quarantine data table
        displayQuarantineTable('quarantineDataTable', quarantine_data);
        
        // Setup download button
        const downloadBtn = document.getElementById('downloadReportBtn');
        downloadBtn.onclick = () => downloadReport(result.session_id);
    }

    function displayDataTable(containerId, data, columns) {
        const container = document.getElementById(containerId);
        
        if (!data || data.length === 0) {
            container.innerHTML = '<div class="no-data">No data available</div>';
            return;
        }
        
        let html = '<table class="table">';
        
        // Header
        html += '<thead><tr>';
        columns.forEach(col => {
            html += `<th>${col.replace('_', ' ').toUpperCase()}</th>`;
        });
        html += '</tr></thead>';
        
        // Body
        html += '<tbody>';
        data.forEach(row => {
            html += '<tr>';
            columns.forEach(col => {
                let value = row[col] || '';
                if (col === 'salary' || col === 'yearly_salary') {
                    value = typeof value === 'number' ? '$' + value.toLocaleString() : value;
                }
                html += `<td>${value}</td>`;
            });
            html += '</tr>';
        });
        html += '</tbody></table>';
        
        container.innerHTML = html;
    }

    function displayQuarantineTable(containerId, data) {
        const container = document.getElementById(containerId);
        
        if (!data || data.length === 0) {
            container.innerHTML = '<div class="no-data">No quarantined records</div>';
            return;
        }
        
        let html = '<table class="table">';
        
        // Header
        html += '<thead><tr>';
        html += '<th>SOURCE FILE</th>';
        html += '<th>ERROR REASON</th>';
        html += '<th>ERROR COLUMN</th>';
        html += '<th>RAW DATA</th>';
        html += '</tr></thead>';
        
        // Body
        html += '<tbody>';
        data.forEach(row => {
            const rawData = JSON.parse(row.raw_data);
            const displayData = Object.entries(rawData)
                .filter(([key, value]) => key !== '_source_file')
                .map(([key, value]) => `${key}: ${value}`)
                .join(', ');
                
            html += '<tr>';
            html += `<td>${row.source_file}</td>`;
            html += `<td>${row.error_reason}</td>`;
            html += `<td>${row.error_column}</td>`;
            html += `<td title="${displayData}">${displayData.length > 50 ? displayData.substring(0, 50) + '...' : displayData}</td>`;
            html += '</tr>';
        });
        html += '</tbody></table>';
        
        container.innerHTML = html;
    }

    function showSection(sectionName) {
        // Hide all sections
        document.getElementById('uploadSection') && (document.getElementById('uploadSection').style.display = 'none');
        document.getElementById('loadingSection').style.display = 'none';
        document.getElementById('resultsSection').style.display = 'none';
        document.getElementById('errorSection').style.display = 'none';
        
        // Show requested section
        if (sectionName === 'loading') {
            document.getElementById('loadingSection').style.display = 'block';
        } else if (sectionName === 'results') {
            document.getElementById('resultsSection').style.display = 'block';
        } else if (sectionName === 'error') {
            document.getElementById('errorSection').style.display = 'block';
        }
    }

    function showError(message) {
        document.getElementById('errorMessage').textContent = message;
        showSection('error');
    }

    function downloadReport(sessionId) {
        window.open(`/download_report/${sessionId}`, '_blank');
    }

    // Global functions for tabs and reset
    window.showTab = function(tabName) {
        // Remove active class from all tabs
        document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
        
        // Add active class to selected tab
        event.target.classList.add('active');
        document.getElementById(tabName + 'Tab').classList.add('active');
    };

    window.resetInterface = function() {
        selectedFiles = [];
        fileList.innerHTML = '';
        fileInput.value = '';
        updateProcessButton();
        
        // Hide all result sections
        document.getElementById('loadingSection').style.display = 'none';
        document.getElementById('resultsSection').style.display = 'none';
        document.getElementById('errorSection').style.display = 'none';
    };

    window.removeFile = removeFile;
});