import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3 # Cambiado de pyodbc a sqlite3
# import decimal # No es necesario para sqlite3, que maneja números como floats/ints

st.set_page_config(page_title="PracticaSQL", layout="wide")

# Nombre del archivo de la base de datos SQLite
NOMBRE_DB = "facturabd.db"

# --- Funciones de Base de Datos SQLite ---

def crear_tablas(conn):
    """Crea las tablas en la base de datos SQLite."""
    cursor = conn.cursor()

    # Habilitar claves foráneas (recomendado para SQLite)
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Crear tablas
    tablas = [
        ("""CREATE TABLE IF NOT EXISTS Estados (
            Id_Estado TEXT PRIMARY KEY NOT NULL,
            Estado_Cte TEXT
        );"""),
        ("""CREATE TABLE IF NOT EXISTS Productos (
            Id_Producto TEXT PRIMARY KEY NOT NULL,
            Desc_Producto TEXT NOT NULL,
            Precio_Unitario REAL NOT NULL
        );"""),
        ("""CREATE TABLE IF NOT EXISTS Clientes (
            Id_Cte TEXT PRIMARY KEY NOT NULL,
            Nombre_Cte TEXT NOT NULL,
            Direccion_Cte TEXT,
            Ciudad_Cte TEXT,
            Id_Estado TEXT NOT NULL,
            CP_Cte TEXT NOT NULL,
            RFC_Cte TEXT NOT NULL,
            FOREIGN KEY (Id_Estado) REFERENCES Estados(Id_Estado)
        );"""),
        ("""CREATE TABLE IF NOT EXISTS Cte_Pedido (
            Id_Pedido TEXT PRIMARY KEY NOT NULL,
            Id_Cte TEXT NOT NULL,
            Fecha_Pedido TEXT, -- Almacenar fecha como texto ISO8601 "YYYY-MM-DD"
            FOREIGN KEY (Id_Cte) REFERENCES Clientes(Id_Cte)
        );"""),
        ("""CREATE TABLE IF NOT EXISTS Detalle_Pedido (
            Id_Pedido TEXT NOT NULL,
            Id_Producto TEXT NOT NULL,
            Cantidad INTEGER NOT NULL,
            PRIMARY KEY (Id_Pedido, Id_Producto),
            FOREIGN KEY (Id_Pedido) REFERENCES Cte_Pedido(Id_Pedido),
            FOREIGN KEY (Id_Producto) REFERENCES Productos(Id_Producto)
        );""")
    ]

    for consulta in tablas:
        cursor.execute(consulta)

    conn.commit()
    st.info("Verificando/creando tablas en la base de datos...") # Cambiado a info para menos intrusividad

def insertar_datos_iniciales(conn):
    """Inserta los datos iniciales en las tablas si no existen."""
    cursor = conn.cursor()

    try:
        # Insertar Estados
        datos_estados = [
            ('E01', 'CDMX'), ('E02', 'Jalisco'), ('E03', 'Nuevo León'),
            ('E04', 'Colima'), ('E05', 'Sinaloa')
        ]
        # Usamos INSERT OR IGNORE para evitar duplicados si se ejecuta varias veces
        cursor.executemany("INSERT OR IGNORE INTO Estados (Id_Estado, Estado_Cte) VALUES (?, ?)", datos_estados)

        # Insertar Clientes
        datos_clientes = [
            ('C01', 'Juan Pérez', 'Calle 1', 'CDMX', 'E01', '01000', 'JUAP850101ABC'),
            ('C02', 'Ana Gómez', 'Av. Siempre Viva', 'Guadalajara', 'E02', '44100', 'ANGG920202XYZ'),
            ('C03', 'Carlos Ruiz', 'Insurgentes Sur 123', 'Monterrey', 'E03', '64000', 'CARU900315DEF'),
            ('C04', 'María Fernández', 'Lopez Mateos 456', 'Colima', 'E04', '28000', 'MAFE880720GHI'),
            ('C05', 'Luis Torres', 'Aquiles Serdán 789', 'Culiacán', 'E05', '80000', 'LUIT951105JKL'),
             ('C06', 'Sofía Ramírez', 'Benito Juárez 101', 'CDMX', 'E01', '06000', 'SORA910418MNO'),
             ('C07', 'Ricardo Morales', 'Revolución 202', 'Guadalajara', 'E02', '44200', 'RIMO870925PQR'),
             ('C08', 'Elena Castro', 'Constitución 303', 'Monterrey', 'E03', '64100', 'ELCA931201STU'),
             ('C09', 'Francisco Jiménez', 'Hidalgo 404', 'Colima', 'E04', '28100', 'FAJI860608VWX'),
             ('C10', 'Gabriela Vargas', 'Zaragoza 505', 'Culiacán', 'E05', '80100', 'GAVA940112YZA'),
             ('C11', 'Pedro Herrera', 'Madero 606', 'CDMX', 'E01', '07000', 'PEHE890830BCD'),
             ('C12', 'Laura Mendoza', 'Carranza 707', 'Guadalajara', 'E02', '44300', 'LAME960203EFG'),
             ('C13', 'Diego Ortiz', 'Morelos 808', 'Monterrey', 'E03', '64200', 'DIOR900517HIJ'),
             ('C14', 'Andrea Navarro', 'Allende 909', 'Colima', 'E04', '28200', 'ANNA921024KLM'),
             ('C15', 'Sergio Soto', 'Juárez 1010', 'Culiacán', 'E05', '80200', 'SESO850307NOP')
        ]
        cursor.executemany("INSERT OR IGNORE INTO Clientes (Id_Cte, Nombre_Cte, Direccion_Cte, Ciudad_Cte, Id_Estado, CP_Cte, RFC_Cte) VALUES (?, ?, ?, ?, ?, ?, ?)", datos_clientes)

        # Insertar Productos
        datos_productos = [
            ('PD01', 'Laptop HP 15"', 12000.00),
            ('PD02', 'Mouse Inalámbrico', 350.00),
            ('PD03', 'Teclado Mecánico', 1500.00),
            ('PD04', 'Monitor 24"', 3000.00),
            ('PD05', 'Impresora Laser', 4500.00),
             ('PD06', 'Disco Duro SSD 1TB', 2000.00),
             ('PD07', 'Memoria RAM 16GB', 1800.00),
             ('PD08', 'Webcam HD', 700.00),
             ('PD09', 'Router WiFi', 900.00),
             ('PD10', 'Tarjeta Gráfica', 8000.00)
        ]
        cursor.executemany("INSERT OR IGNORE INTO Productos (Id_Producto, Desc_Producto, Precio_Unitario) VALUES (?, ?, ?)", datos_productos)

        # Insertar Cte_Pedido (solo algunos ejemplos, las fechas deben ser TEXT "YYYY-MM-DD")
        datos_cte_pedido = [
            ('P001', 'C14', '2024-05-01'),
            ('P002', 'C02', '2024-05-02'),
            ('P003', 'C05', '2024-05-03'),
            ('P004', 'C01', '2024-05-04'),
            ('P005', 'C10', '2024-05-05')
        ]
        cursor.executemany("INSERT OR IGNORE INTO Cte_Pedido (Id_Pedido, Id_Cte, Fecha_Pedido) VALUES (?, ?, ?)", datos_cte_pedido)

        # Insertar Detalle_Pedido
        datos_detalle_pedido = [
            ('P001', 'PD01', 1), ('P001', 'PD02', 2),
            ('P002', 'PD03', 1), ('P002', 'PD04', 1),
            ('P003', 'PD05', 1),
            ('P004', 'PD01', 1), ('P004', 'PD03', 1), ('P004', 'PD06', 1),
            ('P005', 'PD07', 2), ('P005', 'PD08', 1), ('P005', 'PD09', 1)
        ]
        cursor.executemany("INSERT OR IGNORE INTO Detalle_Pedido (Id_Pedido, Id_Producto, Cantidad) VALUES (?, ?, ?)", datos_detalle_pedido)


        conn.commit()
        st.info("Verificando/insertando datos iniciales...") # Cambiado a info

    except sqlite3.IntegrityError as e:
        st.warning(f"Advertencia de integridad al insertar datos (algunos datos podrían ya existir): {e}") # Cambiado a warning
    except sqlite3.Error as e:
        st.error(f"Error al insertar datos iniciales: {e}")

def conectar_a_db():
    """Conecta a la base de datos SQLite y devuelve la conexión.
       Crea tablas e inserta datos iniciales si es la primera vez.
    """
    try:
        conn = sqlite3.connect(NOMBRE_DB)
        # Configurar el row_factory para acceder a las columnas por nombre
        conn.row_factory = sqlite3.Row
        # Habilitar soporte de fechas y tiempos
        conn.execute("PRAGMA foreign_keys = ON;") # Asegurarse de que las FK estén activas en esta conexión
        st.info(f"Conectado a la base de datos: {NOMBRE_DB}")
        return conn
    except sqlite3.Error as e:
        st.error(f"Error con la base de datos: {e}")
        return None

def leer_datos_bd(conn, query, params=None):
    """Lee datos de la base de datos."""
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        # Como usamos row_factory, podemos convertir directamente a dict
        data = [dict(row) for row in cursor.fetchall()]
        # Convertir valores decimales/None si es necesario, aunque SQLite debería dar floats/ints
        # La lógica de conversión de decimal a float y None a 0.0 ya no es tan crítica con sqlite3
        # pero la mantenemos por si acaso, adaptada a los tipos de sqlite3
        processed_data = []
        for row in data:
             processed_row = {}
             for col, val in row.items():
                 if val is None and col in ['Cantidad', 'Precio_Unitario']:
                     processed_row[col] = 0.0
                 elif isinstance(val, (int, float)): # SQLite devuelve int o float
                     processed_row[col] = float(val) # Aseguramos float para cálculos si es necesario
                 elif isinstance(val, str) and col == 'Fecha_Pedido':
                     try:
                         # Intentar parsear la fecha si es una cadena
                         processed_row[col] = datetime.strptime(val, '%Y-%m-%d').date()
                     except ValueError:
                         processed_row[col] = val # Dejar como cadena si falla el parseo
                 else:
                     processed_row[col] = val
             processed_data.append(processed_row)

        return processed_data
    except sqlite3.Error as e:
        st.error(f"Error SELECT: {e}")
        return []

def modificar_datos_bd(conn, query, params=None):
    """Ejecuta INSERT, UPDATE, DELETE en la base de datos."""
    if not conn: return False
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        st.error(f"Error INSERT/UPDATE: {e}")
        return False

def generar_folio(conn):
    """Genera el siguiente número de folio basado en el último en la BD."""
    # Modificado para usar leer_datos_bd
    data = leer_datos_bd(conn, "SELECT MAX(Id_Pedido) AS MaxFolio FROM Cte_Pedido")
    # La columna 'MaxFolio' será None si no hay registros, o el valor TEXT
    if not data or not data[0]['MaxFolio']:
        return "F001"
    folio = data[0]['MaxFolio'].strip() # .strip() puede ser útil si hay espacios
    # Extraer prefijo y número
    prefijo = ''.join(filter(str.isalpha, folio)) or "F" # Si no hay letras, usar 'F'
    numero_str = ''.join(filter(str.isdigit, folio))
    numero = int(numero_str) + 1 if numero_str else 1 # Si no hay números, empezar en 1

    return f"{prefijo}{numero:03d}"

# --- Inicializar la base de datos y cargar datos iniciales ---
db_conn = conectar_a_db()

# Si la conexión es exitosa, asegurar que las tablas y datos iniciales existan
if db_conn:
    crear_tablas(db_conn)
    insertar_datos_iniciales(db_conn)

# --- Cargar datos iniciales para Selectboxes ---
CLIENTES = {row['Id_Cte'].strip(): row for row in leer_datos_bd(db_conn, "SELECT Id_Cte, Nombre_Cte, RFC_Cte FROM Clientes ORDER BY Nombre_Cte")} if db_conn else {}
PRODUCTOS = {row['Desc_Producto']: {'id': row['Id_Producto'].strip(), 'precio': row['Precio_Unitario']} for row in leer_datos_bd(db_conn, "SELECT Id_Producto, Desc_Producto, Precio_Unitario FROM Productos ORDER BY Desc_Producto")} if db_conn else {}

# --- Inicializar sesión ---
st.session_state.setdefault('items_factura', [])
# Generar folio solo si db_conn es válido
st.session_state.setdefault('folio_actual', generar_folio(db_conn) if db_conn else "ERROR_FOLIO")
# Asegurar que los clientes seleccionados sean válidos
st.session_state.setdefault('cliente_seleccionado_factura_id', next(iter(CLIENTES), None))
st.session_state.setdefault('cliente_seleccionado_consulta_id', next(iter(CLIENTES), None))


# --- Sección: Generar Factura ---
st.title("Facturación de Pedidos")
st.subheader("--Generar Nueva Factura--")

# Verificar si la conexión a la BD es válida antes de continuar
if not db_conn:
    st.error("No se pudo conectar a la base de datos. La aplicación no puede continuar.")
else:
    col1, col2 = st.columns(2)
    col1.text_input("Folio", st.session_state.folio_actual, disabled=True)
    fecha_factura = col2.date_input("Fecha de facturación", datetime.now().date()) # Aseguramos que sea un objeto date


    cliente_options = {k: f"{v['Nombre_Cte']} (ID: {k})" for k, v in CLIENTES.items()}
    # Asegurar que el valor predeterminado esté en las opciones
    default_client_factura = st.session_state.cliente_seleccionado_factura_id if st.session_state.cliente_seleccionado_factura_id in cliente_options else (next(iter(cliente_options), None))
    cliente_sel = st.selectbox("Cliente", options=list(cliente_options), format_func=lambda x: cliente_options[x], key="cliente_factura", index=list(cliente_options.keys()).index(default_client_factura) if default_client_factura else 0)

    st.text_input("RFC", CLIENTES.get(cliente_sel, {}).get('RFC_Cte', ''), disabled=True)

    # --- Agregar productos ---
    if PRODUCTOS:
        with st.form("add_item"):
            prod = st.selectbox("Producto", list(PRODUCTOS))
            info = PRODUCTOS[prod]
            st.write(f"ID: {info['id']} | Precio: ${info['precio']:.2f}")
            cantidad = st.number_input("Cantidad", min_value=1, step=1, value=1)
            if st.form_submit_button("Agregar"):
                st.session_state.items_factura.append({
                    "id_producto_db": info['id'], "descripcion": prod,
                    "cantidad": cantidad, "precio_unitario": info['precio'],
                    "subtotal_item": cantidad * info['precio']
                })
                st.success(f"{prod} agregado.")
    else:
        st.warning("No se pudieron cargar los productos de la base de datos.")


    # --- Mostrar factura actual ---
    if st.session_state.items_factura:
        st.subheader("Detalle de la Factura")
        # Asegurarse de que las columnas numéricas sean del tipo correcto para cálculos
        df = pd.DataFrame(st.session_state.items_factura)
        df['cantidad'] = pd.to_numeric(df['cantidad'])
        df['precio_unitario'] = pd.to_numeric(df['precio_unitario'])
        df['subtotal_item'] = df['cantidad'] * df['precio_unitario'] # Recalcular por si acaso

        st.dataframe(df.rename(columns={"descripcion":"Producto", "cantidad":"Cant.", "precio_unitario":"P.U.", "subtotal_item":"Subtotal"}), hide_index=True)
        subtotal = df['subtotal_item'].sum() # Usar la columna recalculada
        iva = subtotal * 0.16
        total = subtotal + iva
        col1, col2, col3 = st.columns(3)
        col1.metric("Subtotal", f"${subtotal:,.2f}")
        col2.metric("IVA", f"${iva:,.2f}")
        col3.metric("Total", f"${total:,.2f}")

        col_save, col_new = st.columns(2)
        if col_save.button("Guardar Factura"):
            # Formatear la fecha a TEXT "YYYY-MM-DD" para SQLite
            fecha_str = fecha_factura.strftime('%Y-%m-%d')
            if modificar_datos_bd(db_conn, "INSERT INTO Cte_Pedido (Id_Pedido, Id_Cte, Fecha_Pedido) VALUES (?, ?, ?)", (st.session_state.folio_actual, cliente_sel, fecha_str)):
                errores = False
                for item in st.session_state.items_factura:
                    ok = modificar_datos_bd(db_conn, "INSERT INTO Detalle_Pedido (Id_Pedido, Id_Producto, Cantidad) VALUES (?, ?, ?)", (st.session_state.folio_actual, item['id_producto_db'], item['cantidad']))
                    if not ok:
                        errores = True
                        # Considerar eliminar el Cte_Pedido recién insertado si falla el detalle
                        modificar_datos_bd(db_conn, "DELETE FROM Cte_Pedido WHERE Id_Pedido = ?", (st.session_state.folio_actual,))
                        st.error(f"Error al guardar detalle para {item['descripcion']}. La factura no se guardó completamente.")
                        break
                if not errores:
                    st.success("Factura guardada.")
                    st.session_state.items_factura = []
                    st.session_state.folio_actual = generar_folio(db_conn)
                    st.rerun() # Recargar la página para limpiar el formulario y actualizar el folio

        if col_new.button("Nueva Factura"):
            st.session_state.items_factura = []
            st.session_state.folio_actual = generar_folio(db_conn)
            st.rerun() # Recargar la página para limpiar el formulario y actualizar el folio

    elif not PRODUCTOS:
         st.warning("No hay productos disponibles para agregar a la factura.")


    # --- Sección: Consultar Pedidos ---
    st.markdown("---")
    st.subheader("Consultar Cliente y Pedidos")

    # Asegurar que el valor predeterminado esté en las opciones
    default_client_consulta = st.session_state.cliente_seleccionado_consulta_id if st.session_state.cliente_seleccionado_consulta_id in cliente_options else (next(iter(cliente_options), None))
    cliente_consulta = st.selectbox("Cliente a Consultar", options=list(cliente_options), format_func=lambda x: cliente_options[x], key="cliente_consulta", index=list(cliente_options.keys()).index(default_client_consulta) if default_client_consulta else 0)


    def mostrar_detalles_pedido(pedido_id):
        detalles = leer_datos_bd(db_conn, """
        SELECT
            dp.Id_Pedido,
            p.Id_Producto AS Codigo_Producto,
            p.Desc_Producto,
            dp.Cantidad,
            p.Precio_Unitario AS Precio_Unitario,
            (dp.Cantidad * p.Precio_Unitario) AS Subtotal,
            -- Calcular IVA y Total aquí también, aunque no se almacenen directamente
            (dp.Cantidad * p.Precio_Unitario * 0.16) AS IVA_Aplicado,
            (dp.Cantidad * p.Precio_Unitario * 1.16) AS Total_Con_IVA
        FROM Detalle_Pedido dp
        JOIN Productos p ON dp.Id_Producto = p.Id_Producto
        WHERE dp.Id_Pedido = ?
        ORDER BY p.Desc_Producto;
    """, (pedido_id,))

        if detalles:
            df = pd.DataFrame(detalles)
            df = df.rename(columns={
                "Codigo_Producto": "Código",
                "Desc_Producto": "Producto",
                "Cantidad": "Cant.",
                "Precio_Unitario": "P.U.",
                "Subtotal": "Subtotal",
                "IVA_Aplicado": "IVA",
                "Total_Con_IVA": "Total"
            })
            # Formatear columnas numéricas para mostrar como moneda
            for col in ["P.U.", "Subtotal", "IVA", "Total"]:
                 if col in df.columns:
                      df[col] = df[col].apply(lambda x: f"${x:,.2f}")
            st.dataframe(df, hide_index=True)
        else:
            st.info("Sin detalles para este pedido.")

    cliente_info = leer_datos_bd(db_conn, """
        SELECT
            c.Id_Cte,
            c.Nombre_Cte,
            c.Direccion_Cte,
            c.Ciudad_Cte,
            e.Estado_Cte,
            c.RFC_Cte
        FROM Clientes c
        JOIN Estados e ON c.Id_Estado = e.Id_Estado
        WHERE c.Id_Cte = ?
    """, (cliente_consulta,))


    if cliente_info:
        c = cliente_info[0]
        st.markdown(f"**{c['Nombre_Cte']} (ID: {c['Id_Cte'].strip()})**")
        st.text(f"RFC: {c['RFC_Cte']} | Dirección: {c['Direccion_Cte'] or 'N/A'}, {c['Ciudad_Cte'] or ''}, {c['Estado_Cte'] or ''}")

        pedidos = leer_datos_bd(db_conn, "SELECT Id_Pedido, Fecha_Pedido FROM Cte_Pedido WHERE Id_Cte = ? ORDER BY Fecha_Pedido DESC", (cliente_consulta,))
        if pedidos:
            for p in pedidos:
                # Asegurarse de que Fecha_Pedido sea un objeto date si se recupera como tal
                fecha_pedido_str = p['Fecha_Pedido']
                if isinstance(fecha_pedido_str, str):
                     try:
                         fecha_pedido_obj = datetime.strptime(fecha_pedido_str, '%Y-%m-%d').date()
                     except ValueError:
                         fecha_pedido_obj = fecha_pedido_str # Mantener como string si hay error
                else:
                     fecha_pedido_obj = fecha_pedido_str # Ya es un objeto date o similar

                # Mostrar la fecha formateada
                fecha_formateada = fecha_pedido_obj.strftime('%Y-%m-%d') if isinstance(fecha_pedido_obj, datetime) else str(fecha_pedido_obj)

                with st.expander(f"Pedido {p['Id_Pedido'].strip()} - {fecha_formateada}"):
                    mostrar_detalles_pedido(p['Id_Pedido'])
        else:
            st.info("Este cliente no tiene pedidos.")
    else:
        st.warning("Cliente no encontrado.")