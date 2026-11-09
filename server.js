import express from 'express';
import multer from 'multer';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import ws from 'ws';

dotenv.config();

const app = express();
const upload = multer({ dest: 'uploads/' });

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: { persistSession: false },
    realtime: { transport: ws }
  }
);

const BUCKET_NAME = 'uploads';
const CHUNK_SIZE = 40 * 1024 * 1024; // 40 MB por parte
const MAX_STORAGE_LIMIT = 1 * 1024 * 1024 * 1024; // 1 GB (Plan Gratuito Supabase)

app.use(express.static('public'));
app.use(express.json());

// Función corregida para listar absolutamente todos los archivos de metadata
async function getAllFiles() {
  const { data, error } = await supabase.storage.from(BUCKET_NAME).list('metadata', {
    limit: 1000,
    sortBy: { column: 'created_at', order: 'desc' }
  });
  
  if (error || !data) return [];
  
  const files = [];
  for (const item of data) {
    if (item.name.endsWith('.json')) {
      const { data: fileData } = await supabase.storage.from(BUCKET_NAME).download(`metadata/${item.name}`);
      if (fileData) {
        try {
          const text = await fileData.text();
          files.push(JSON.parse(text));
        } catch (e) {
          console.error(`Error leyendo metadata ${item.name}:`, e);
        }
      }
    }
  }
  return files.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
}
// Función auxiliar para eliminar físicamente un archivo y sus partes de Supabase
async function deletePhysicalFile(fileRecord) {
  try {
    const pathsToDelete = fileRecord.parts.map(p => `chunks/${p}`);
    pathsToDelete.push(`metadata/${fileRecord.slug}.json`);
    await supabase.storage.from(BUCKET_NAME).remove(pathsToDelete);
  } catch (err) {
    console.error(`Error borrando archivos físicos de ${fileRecord.slug}:`, err);
  }
}

// Modificar la ruta de listado de archivos para limpiar automáticamente los vencidos hace más de 1 día
app.get('/api/files', async (req, res) => {
  try {
    const files = await getAllFiles();
    const now = new Date();
    const validFiles = [];

    for (const file of files) {
      let shouldDelete = false;
      const oneDayMs = 24 * 60 * 60 * 1000;

      // Condición 1: Expiró por tiempo + 1 día extra
      if (file.expiresAt && now > new Date(new Date(file.expiresAt).getTime() + oneDayMs)) {
        shouldDelete = true;
      }

      // Condición 2: Se agotaron las descargas + 1 día extra desde su creación o última descarga
      if (file.maxDownloads !== null && file.currentDownloads >= file.maxDownloads) {
        // Si no tiene expiresAt, usamos un estimado o borramos 1 día después de completarse
        const completedTime = file.completedAt ? new Date(file.completedAt).getTime() : new Date(file.createdAt).getTime();
        if (now > new Date(completedTime + oneDayMs)) {
          shouldDelete = true;
        }
      }

      if (shouldDelete) {
        await deletePhysicalFile(file);
      } else {
        validFiles.push(file);
      }
    }

    const totalUsed = validFiles.reduce((acc, file) => acc + (file.totalSize || 0), 0);
    res.json({ files: validFiles, storageUsed: totalUsed, storageLimit: MAX_STORAGE_LIMIT });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
async function getFileBySlug(slug) {
  try {
    const { data, error } = await supabase.storage.from(BUCKET_NAME).download(`metadata/${slug}.json`);
    if (error || !data) return null;
    return JSON.parse(await data.text());
  } catch {
    return null;
  }
}

async function saveFileMetadata(fileRecord) {
  await supabase.storage.from(BUCKET_NAME).upload(
    `metadata/${fileRecord.slug}.json`, 
    JSON.stringify(fileRecord, null, 2), 
    { contentType: 'application/json', upsert: true }
  );
}

// 1. API: Listar archivos
app.get('/api/files', async (req, res) => {
  try {
    const files = await getAllFiles();
    const totalUsed = files.reduce((acc, file) => acc + (file.totalSize || 0), 0);
    res.json({ files, storageUsed: totalUsed, storageLimit: MAX_STORAGE_LIMIT });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. API: Subir archivo y fragmentarlo
app.post('/api/upload', upload.single('file'), async (req, res) => {
  let filePath = req.file ? req.file.path : null;
  try {
    if (!req.file) return res.status(400).json({ error: 'No se subió ningún archivo' });

    let { customSlug, expiryMode, maxDownloads, expiresInHours } = req.body;
    const originalName = req.file.originalname;
    
    let slug = customSlug ? customSlug.trim().toLowerCase().replace(/[^a-z0-9-_]/g, '-') : '';
    
    if (!slug) {
      slug = Date.now().toString(36) + '-' + Math.random().toString(36).substring(2, 7);
    } else {
      const existing = await getFileBySlug(slug);
      if (existing) {
        if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
        return res.status(400).json({ error: 'El nombre personalizado o URL ya está en uso.' });
      }
    }

    const fileStats = fs.statSync(filePath);
    const fileBuffer = fs.readFileSync(filePath);

    const allFiles = await getAllFiles();
    const currentUsed = allFiles.reduce((acc, f) => acc + f.totalSize, 0);
    if (currentUsed + fileStats.size > MAX_STORAGE_LIMIT) {
      if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
      return res.status(400).json({ error: 'Límite de almacenamiento de 1 GB excedido.' });
    }

    const totalParts = Math.ceil(fileStats.size / CHUNK_SIZE);
    const fileId = 'file-' + Date.now();
    const parts = [];

    for (let i = 0; i < totalParts; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, fileStats.size);
      const chunkBuffer = fileBuffer.subarray(start, end);
      const partName = `${fileId}.part${i + 1}`;

      const { error } = await supabase.storage
        .from(BUCKET_NAME)
        .upload(`chunks/${partName}`, chunkBuffer, { contentType: 'application/octet-stream', upsert: true });

      if (error) throw new Error(`Error en parte ${i + 1}: ${error.message}`);
      parts.push(partName);
    }

    let expiresAt = null;
    if (expiryMode === 'time' || expiryMode === 'both') {
      const hours = parseFloat(expiresInHours) || 24;
      expiresAt = new Date(Date.now() + hours * 60 * 60 * 1000).toISOString();
    }

    let downloadLimit = null;
    if (expiryMode === 'downloads' || expiryMode === 'both') {
      downloadLimit = parseInt(maxDownloads) || 1;
    }

    const fileRecord = {
      slug,
      fileId,
      originalName,
      totalSize: fileStats.size,
      totalParts,
      parts,
      expiryMode,
      maxDownloads: downloadLimit,
      currentDownloads: 0,
      expiresAt,
      createdAt: new Date().toISOString()
    };

    await saveFileMetadata(fileRecord);
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);

    res.json({ success: true, slug });
  } catch (err) {
    console.error(err);
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
    res.status(500).json({ error: err.message });
  }
});

// 3. API: Eliminar archivo
app.delete('/api/files/:slug', async (req, res) => {
  try {
    const { slug } = req.params;
    const fileRecord = await getFileBySlug(slug);
    if (!fileRecord) return res.status(404).json({ error: 'Archivo no encontrado' });

    const pathsToDelete = fileRecord.parts.map(p => `chunks/${p}`);
    pathsToDelete.push(`metadata/${slug}.json`);
    
    await supabase.storage.from(BUCKET_NAME).remove(pathsToDelete);
    res.json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// 4. Ruta GET /:slug -> Vista previa interactiva con barra de porcentaje y tiempos/descargas restantes
app.get('/:slug', async (req, res, next) => {
  const { slug } = req.params;
  if (slug === 'api' || slug.includes('.')) return next();

  const fileRecord = await getFileBySlug(slug);
  if (!fileRecord) {
    return res.status(404).send(renderErrorPage('Archivo no encontrado', 'El enlace que buscas no existe o fue eliminado.'));
  }

  const now = new Date();
  const oneDayMs = 24 * 60 * 60 * 1000;

  // Autodestrucción si pasó más de 1 día de su expiración por tiempo
  if (fileRecord.expiresAt && now > new Date(new Date(fileRecord.expiresAt).getTime() + oneDayMs)) {
    await deletePhysicalFile(fileRecord);
    return res.status(410).send(renderErrorPage('Archivo Eliminado', 'Este archivo expiró y fue eliminado permanentemente.'));
  }

  // Autodestrucción si pasó más de 1 día desde que se completaron las descargas
  if (fileRecord.maxDownloads !== null && fileRecord.currentDownloads >= fileRecord.maxDownloads) {
    const limitTime = fileRecord.completedAt ? new Date(fileRecord.completedAt).getTime() : new Date(fileRecord.createdAt).getTime();
    if (now > new Date(limitTime + oneDayMs)) {
      await deletePhysicalFile(fileRecord);
      return res.status(410).send(renderErrorPage('Archivo Eliminado', 'Se alcanzó el límite de descargas y el archivo fue eliminado permanentemente.'));
    }
  }

  // Validaciones de expiración estándar
  if (fileRecord.expiresAt && now > new Date(fileRecord.expiresAt)) {
    return res.status(410).send(renderErrorPage('Archivo Expirado', 'Este archivo ha superado su tiempo límite de disponibilidad.'));
  }

  if (fileRecord.maxDownloads !== null && fileRecord.currentDownloads >= fileRecord.maxDownloads) {
    return res.status(410).send(renderErrorPage('Límite de Descargas Alcanzado', 'Este archivo ya alcanzó el número máximo de descargas permitidas.'));
  }

  const sizeMB = (fileRecord.totalSize / (1024 * 1024)).toFixed(2);

  // Calcular tiempo restante si aplica
  let remainingTimeText = null;
  if (fileRecord.expiresAt) {
    const diffMs = new Date(fileRecord.expiresAt) - now;
    if (diffMs > 0) {
      const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
      const diffMins = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
      remainingTimeText = diffHours > 0 ? `${diffHours}h ${diffMins}m` : `${diffMins} minutos`;
    } else {
      remainingTimeText = 'Expirado';
    }
  }

  // Calcular descargas restantes si aplica
  let remainingDownloadsText = 'Ilimitadas';
  if (fileRecord.maxDownloads !== null) {
    const left = fileRecord.maxDownloads - fileRecord.currentDownloads;
    remainingDownloadsText = `${left > 0 ? left : 0} de ${fileRecord.maxDownloads}`;
  }

  res.send(`<!DOCTYPE html>
<html lang="es" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <title>Descargar ${fileRecord.originalName} - TransferMe</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", sans-serif; }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex items-center justify-center p-4">
    <div class="bg-slate-900/90 border border-slate-800/80 rounded-3xl p-6 sm:p-8 max-w-sm w-full shadow-2xl backdrop-blur-2xl text-center space-y-6">
        <div class="w-14 h-14 bg-indigo-600/20 text-indigo-400 rounded-2xl flex items-center justify-center mx-auto text-sm font-bold tracking-wider">FILE</div>
        <div>
            <h1 class="text-base font-semibold text-white break-words">${fileRecord.originalName}</h1>
            <p class="text-xs text-slate-400 mt-1">Listo para descargar de forma segura</p>
        </div>
        
        <div class="bg-slate-950/80 rounded-2xl p-4 text-left space-y-2 text-xs text-slate-300 border border-slate-800/80">
            <div class="flex justify-between"><span>Tamaño:</span> <span class="font-mono text-white">${sizeMB} MB</span></div>
            ${fileRecord.maxDownloads !== null ? `<div class="flex justify-between"><span>Descargas Restantes:</span> <span class="font-mono text-white">${remainingDownloadsText}</span></div>` : ''}
            ${fileRecord.expiresAt ? `<div class="flex justify-between"><span>Tiempo Restante:</span> <span class="font-mono text-white">${remainingTimeText}</span></div>` : ''}
            ${fileRecord.expiryMode === 'permanent' ? `<div class="flex justify-between"><span>Tipo:</span> <span class="text-white">Permanente</span></div>` : ''}
        </div>

        <!-- Barra de progreso de descarga en tiempo real -->
        <div id="downloadProgressBox" class="hidden space-y-2 text-left">
            <div class="flex justify-between text-xs text-slate-400">
                <span id="dlStatus" class="font-medium text-indigo-300">Descargando...</span>
                <span id="dlPercent" class="font-mono text-indigo-400 font-bold">0%</span>
            </div>
            <div class="w-full bg-slate-950 rounded-full h-2.5 border border-slate-800 overflow-hidden">
                <div id="dlBar" class="bg-indigo-600 h-full transition-all duration-75" style="width: 0%"></div>
            </div>
        </div>

        <button id="downloadBtn" onclick="startDownload('${slug}', '${encodeURIComponent(fileRecord.originalName)}')" class="block w-full py-3.5 px-4 bg-indigo-600 hover:bg-indigo-500 active:scale-98 text-white font-medium rounded-2xl shadow-lg shadow-indigo-600/25 transition-all text-xs text-center">
            Descargar Archivo Ahora
        </button>
    </div>

    <script>
        function startDownload(slug, filename) {
            const btn = document.getElementById('downloadBtn');
            const box = document.getElementById('downloadProgressBox');
            const bar = document.getElementById('dlBar');
            const percent = document.getElementById('dlPercent');
            const status = document.getElementById('dlStatus');

            btn.disabled = true;
            btn.classList.add('opacity-50', 'cursor-not-allowed');
            box.classList.remove('hidden');

            const xhr = new XMLHttpRequest();
            xhr.open('GET', '/api/download/' + slug, true);
            xhr.responseType = 'blob';

            xhr.onprogress = (event) => {
                if (event.lengthComputable) {
                    const p = Math.round((event.loaded / event.total) * 100);
                    bar.style.width = p + '%';
                    percent.textContent = p + '%';
                    
                    if (p < 100) {
                        status.textContent = 'Descargando... (' + p + '%)';
                    } else {
                        status.textContent = 'Uniendo y guardando...';
                    }
                } else {
                    status.textContent = 'Descargando archivo...';
                }
            };

            xhr.onload = () => {
                if (xhr.status === 200) {
                    const blob = xhr.response;
                    const link = document.createElement('a');
                    link.href = window.URL.createObjectURL(blob);
                    link.download = decodeURIComponent(filename);
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);

                    status.textContent = 'Descarga finalizada';
                    percent.textContent = '100%';
                    bar.style.width = '100%';
                    
                    btn.disabled = false;
                    btn.classList.remove('opacity-50', 'cursor-not-allowed');
                    btn.textContent = 'Descargar Nuevamente';
                } else {
                    alert('Error al descargar el archivo.');
                    btn.disabled = false;
                    btn.classList.remove('opacity-50', 'cursor-not-allowed');
                }
            };

            xhr.onerror = () => {
                alert('Error de red durante la descarga.');
                btn.disabled = false;
                btn.classList.remove('opacity-50', 'cursor-not-allowed');
            };

            xhr.send();
        }
    </script>
</body>
</html>`);
});
// 5. Descarga real
app.get('/api/download/:slug', async (req, res) => {
  try {
    const { slug } = req.params;
    const fileRecord = await getFileBySlug(slug);

    if (!fileRecord) return res.status(404).json({ error: 'Archivo no encontrado' });

    if (fileRecord.expiresAt && new Date() > new Date(fileRecord.expiresAt)) {
      return res.status(410).send('Archivo expirado');
    }

    // Validar límite antes de iniciar
    if (fileRecord.maxDownloads !== null && fileRecord.currentDownloads >= fileRecord.maxDownloads) {
      return res.status(410).send('Límite de descargas alcanzado');
    }

    // Descargar y unir todas las partes desde Supabase Storage
    const buffers = [];
    for (const partName of fileRecord.parts) {
      const { data: partData, error: partError } = await supabase.storage
        .from(BUCKET_NAME)
        .download(`chunks/${partName}`);

      if (partError) throw new Error(`Error descargando parte`);
      buffers.push(Buffer.from(await partData.arrayBuffer()));
    }

    const completeBuffer = Buffer.concat(buffers);
    const totalSize = completeBuffer.length;
    const range = req.headers.range;

    let isCompleteDownload = false;

    // Soportar peticiones de rango (HTTP Range) para pausar y reanudar
    if (range) {
      const parts = range.replace(/bytes=/, "").split("-");
      const start = parseInt(parts[0], 10);
      const end = parts[1] ? parseInt(parts[1], 10) : totalSize - 1;

      if (start >= totalSize) {
        res.status(416).setHeader('Content-Range', `bytes */${totalSize}`).send('Requested Range Not Satisfiable');
        return;
      }

      const chunksize = (end - start) + 1;
      const slicedBuffer = completeBuffer.subarray(start, end + 1);

      // Si el rango llega hasta el final del archivo, se considera descarga completa
      if (end >= totalSize - 1) {
        isCompleteDownload = true;
      }

      res.writeHead(206, {
        'Content-Range': `bytes ${start}-${end}/${totalSize}`,
        'Accept-Ranges': 'bytes',
        'Content-Length': chunksize,
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${encodeURIComponent(fileRecord.originalName)}"`
      });
      res.end(slicedBuffer);
    } else {
      isCompleteDownload = true;
      res.writeHead(200, {
        'Content-Length': totalSize,
        'Accept-Ranges': 'bytes',
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${encodeURIComponent(fileRecord.originalName)}"`
      });
      res.end(completeBuffer);
    }

    // Incrementar el contador de descargas ÚNICAMENTE cuando la respuesta finaliza con éxito y llega al final
    res.on('finish', async () => {
      if (isCompleteDownload && res.statusCode < 400) {
        try {
          const currentRecord = await getFileBySlug(slug);
          if (currentRecord) {
            currentRecord.currentDownloads += 1;
            // Si con esta descarga se alcanza el límite, guardamos la marca de tiempo exacta
            if (currentRecord.maxDownloads !== null && currentRecord.currentDownloads >= currentRecord.maxDownloads) {
              currentRecord.completedAt = new Date().toISOString();
            }
            await saveFileMetadata(currentRecord);
          }
        } catch (err) {
          console.error('Error actualizando contador:', err);
        }
      }
    });

  } catch (err) {
    console.error(err);
    if (!res.headersSent) res.status(500).send('Error interno');
  }
});
// API: Editar política de tiempo o descargas de un archivo existente
app.put('/api/files/:slug', async (req, res) => {
  try {
    const { slug } = req.params;
    const { expiryMode, maxDownloads, expiresInHours } = req.body;
    
    const fileRecord = await getFileBySlug(slug);
    if (!fileRecord) return res.status(404).json({ error: 'Archivo no encontrado' });

    fileRecord.expiryMode = expiryMode;
    
    // Actualizar límite de descargas
    if (expiryMode === 'downloads' || expiryMode === 'both') {
      fileRecord.maxDownloads = maxDownloads !== null && maxDownloads !== '' ? parseInt(maxDownloads) : null;
    } else {
      fileRecord.maxDownloads = null;
    }

    // Actualizar tiempo de expiración (calculando nuevas horas desde el momento actual)
    if (expiryMode === 'time' || expiryMode === 'both') {
      const hours = parseFloat(expiresInHours) || 24;
      fileRecord.expiresAt = new Date(Date.now() + hours * 60 * 60 * 1000).toISOString();
    } else {
      fileRecord.expiresAt = null;
    }

    await saveFileMetadata(fileRecord);
    res.json({ success: true, message: 'Archivo actualizado correctamente' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});
function renderErrorPage(title, message) {
  return `<!DOCTYPE html>
<html lang="es" class="dark">
<head><meta charset="UTF-8"><title>${title}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex items-center justify-center p-6">
    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-8 max-w-md w-full text-center space-y-4">
        <h1 class="text-xl font-bold text-rose-400">${title}</h1>
        <p class="text-sm text-slate-400">${message}</p>
    </div>
</body>
</html>`;
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en http://localhost:${PORT}`);
});