from __future__ import annotations
import flet as ft
import pyodbc
import os
import shutil
import hashlib
import base64
from typing import Tuple, Optional, Dict
from datetime import datetime, timedelta
from PIL import Image
import sys
import io
import threading
import time
from OCR import PrecisionExtractor

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()

class DatabaseConfig:
    DRIVER = "ODBC Driver 17 for SQL Server"
    SERVER = "HADJER\\SQLEXPRESS"
    DATABASE = "APPRENTISSAGE"
    
    @staticmethod
    def get_connection() -> Optional[pyodbc.Connection]:
        try:
            return pyodbc.connect(
                f"DRIVER={{{DatabaseConfig.DRIVER}}};"
                f"SERVER={DatabaseConfig.SERVER};"
                f"DATABASE={DatabaseConfig.DATABASE};"
                "Trusted_Connection=yes;"
            )
        except Exception as e:
            print(f"❌ Erreur de connexion: {e}")
            return None

class PhotoManager:
    PHOTOS_ROOT_DIR = os.path.join(BASE_DIR, "data", "apprentis_photos")    
    MAX_PHOTO_SIZE_MB = 5
    ALLOWED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp'}
 
    @staticmethod
    def ensure_photo_directory():
        os.makedirs(PhotoManager.PHOTOS_ROOT_DIR, exist_ok=True)
 
    @staticmethod
    def is_valid_image(file_path: str) -> Tuple[bool, str]:
        if not os.path.exists(file_path):
            return False, "Fichier non trouvé"
 
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in PhotoManager.ALLOWED_EXTENSIONS:
            return False, "Extension non autorisée"
 
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > PhotoManager.MAX_PHOTO_SIZE_MB:
            return False, f"Fichier trop volumineux ({file_size_mb:.1f} MB)"
 
        try:
            with Image.open(file_path) as img:
                img.verify()
            return True, "✅ Image valide"
        except Exception as e:
            return False, f"Fichier corrompu : {e}"
 
    @staticmethod
    def save_photo(apprenti_id: int, source_file_path: str) -> Tuple[bool, str]:
        is_valid, message = PhotoManager.is_valid_image(source_file_path)
        if not is_valid:
            return False, message
 
        try:
            PhotoManager.ensure_photo_directory()
            ext = os.path.splitext(source_file_path)[1].lower()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
            filename = f"{apprenti_id}_{timestamp}{ext}"
            dest_path = os.path.join(PhotoManager.PHOTOS_ROOT_DIR, filename)
            shutil.copy2(source_file_path, dest_path)
            relative_path = f"apprentis_photos/{filename}"
            print(f"✅ Photo sauvegardée : {relative_path}")
            return True, relative_path
        except Exception as e:
            error_msg = f"Erreur sauvegarde photo : {e}"
            print(f"❌ {error_msg}")
            return False, error_msg
 
    @staticmethod
    def get_photo_display_path(photo_path: str) -> str:
        if not photo_path:
            return "assets/default_avatar.png"
 
        if photo_path.startswith("apprentis_photos/"):
            full_path = os.path.join(
                PhotoManager.PHOTOS_ROOT_DIR,
                photo_path.replace("apprentis_photos/", "")
            )
        else:
            full_path = os.path.join(PhotoManager.PHOTOS_ROOT_DIR, photo_path)
 
        return full_path if os.path.exists(full_path) else "assets/default_avatar.png"
 
    @staticmethod
    def delete_photo(photo_path: str) -> bool:
        if not photo_path:
            return True
        try:
            if photo_path.startswith("apprentis_photos/"):
                full_path = os.path.join(
                    PhotoManager.PHOTOS_ROOT_DIR,
                    photo_path.replace("apprentis_photos/", "")
                )
            else:
                full_path = os.path.join(PhotoManager.PHOTOS_ROOT_DIR, photo_path)
 
            if os.path.exists(full_path):
                os.remove(full_path)
                print(f"✅ Photo supprimée : {full_path}")
        except Exception as e:
            print(f"⚠️ Erreur suppression photo : {e}")
        return True
 

def get_commune_id(commune_name: str) -> int:
    if not commune_name:
        return 1
    conn = DatabaseConfig.get_connection()
    if not conn:
        return 1
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ID FROM COMMUNES WHERE LTRIM(RTRIM(LIB_COMMUNE)) = ?",
            (commune_name.strip(),)
        )
        result = cursor.fetchone()
        return result[0] if result else 1
    except Exception:
        return 1
    finally:
        conn.close()
def get_employeur_default() -> dict:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return {}
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                E.ID,
                E.DENOMINATION,
                E.STATUT_JURIDIQUE,
                E.ADRESS,
                E.TELEPHONE,
                E.FAX,
                E.EMAIL,
                E.ID_COM,
                C.LIB_COMMUNE,
                NE.nat_employeur
            FROM EMPLOYEUR E
            LEFT JOIN COMMUNES C ON E.ID_COM = C.ID
            LEFT JOIN NAT_EMPLOYEUR NE ON E.ID_NAT_EMPLY = NE.ID
            WHERE E.DENOMINATION LIKE '%BTPH%'
            ORDER BY E.ID
            OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
        """)
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "id":               row[0],
            "denomination":     row[1] or "",
            "statut_juridique": row[2] or "",
            "adresse":          row[3] or "",
            "telephone":        row[4] or "",
            "fax":              row[5] or "",
            "email":            row[6] or "",
            "id_com":           str(row[7]) if row[7] else None,
            "commune":          row[8] or "",
            "nat_employeur":    row[9] or "Privé",
        }
    except Exception as e:
        print(f"Erreur chargement employeur default: {e}")
        return {}
    finally:
        conn.close()

def get_wilaya_id(wilaya_name: str) -> int:
    if not wilaya_name:
        return 1
    conn = DatabaseConfig.get_connection()
    if not conn:
        return 1
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ID FROM WILAYA WHERE LTRIM(RTRIM(LIB_WILAYA)) = ?",
            (wilaya_name.strip(),)
        )
        result = cursor.fetchone()
        return result[0] if result else 1
    except Exception:
        return 1
    finally:
        conn.close()
def get_wilaya_by_commune_id(commune_id) -> Optional[int]:
    if not commune_id: 
        return None
    conn = DatabaseConfig.get_connection()
    if not conn: 
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT code_commune FROM COMMUNES WHERE ID = ?", (commune_id,))
        row = cursor.fetchone()
        if not row: 
            return None
            
        code_commune = row[0] or ''
        if len(code_commune) < 2:
            return 1  
            
        wilaya_code = code_commune[:2]
        
        cursor.execute("SELECT ID FROM WILAYA WHERE CODE_WIL = ?", (wilaya_code,))
        w_row = cursor.fetchone()
        return w_row[0] if w_row else 1
        
    except Exception as e:
        print(f"Erreur wilaya par commune : {e}")
        return 1
    finally:
        conn.close()

def get_specialite_id(specialite_name: str) -> Optional[int]:
    if not specialite_name:
        return None
    conn = DatabaseConfig.get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ID FROM SPECIALITE WHERE LTRIM(RTRIM(LIB_SSP)) = ? OR code_sp = ?",
            (specialite_name.strip(), specialite_name.strip())
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception:
        return None
    finally:
        conn.close()


def get_niveau_id(niveau_name: str) -> Optional[int]:
    if not niveau_name:
        return None
    conn = DatabaseConfig.get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ID FROM NIVEAU WHERE LTRIM(RTRIM(num_niveau)) = ?",
            (niveau_name.strip(),)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception:
        return None
    finally:
        conn.close()


def get_sexe_id(sexe_value: str) -> Optional[int]:
    if sexe_value == "M":
        return 1
    elif sexe_value == "F":
        return 2
    return None


def get_nat_employeur_id(nat_value: str) -> Optional[int]:
    if not nat_value:
        return None
    conn = DatabaseConfig.get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ID FROM NAT_EMPLOYEUR WHERE LTRIM(RTRIM(nat_employeur)) = ?",
            (nat_value.strip(),)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception:
        return None
    finally:
        conn.close()
def get_moyen_nature_id(moyen_id):
    conn = DatabaseConfig.get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID_NMY FROM MOYEN WHERE ID = ?", (moyen_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    except:
        return None
    finally:
        conn.close()

def get_moyens_apprenti(apprenti_id):
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ID FROM AFFECTATION WHERE ID_APP = ?
        """, (apprenti_id,))
        af_rows = cursor.fetchall()
        if not af_rows:
            return []

        af_ids = [str(r[0]) for r in af_rows]
        placeholders = ", ".join(["?" for _ in af_ids])

        cursor.execute(f"""
            SELECT 
                am.ID,
                m.CODE,
                m.DESIGNATION,
                m.[U.M],
                am.QUANTITE,
                am.PRIX_U,
                CONVERT(VARCHAR, am.DATE_MY, 103) AS DATE_MY
            FROM [AFFECTATION MOYEN] am
            INNER JOIN MOYEN m ON m.ID = am.ID_MY
            WHERE am.ID_AF IN ({placeholders})
            ORDER BY am.DATE_MY DESC
        """, af_ids)

        cols = [col[0] for col in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    except Exception as e:
        print(f"Erreur get_moyens_apprenti: {e}")
        return []
    finally:
        conn.close()

PDF_ROOT = os.path.join(BASE_DIR, "data", "pdfs")

def save_pdf_apprenti(apprenti_id, src_path, nom_fichier):
    try:
        dest_dir = os.path.join(PDF_ROOT, str(apprenti_id))
        os.makedirs(dest_dir, exist_ok=True)

        base, ext = os.path.splitext(nom_fichier)
        dest_path = os.path.join(dest_dir, nom_fichier)
        counter = 1
        while os.path.exists(dest_path):
            dest_path = os.path.join(dest_dir, f"{base}_{counter}{ext}")
            counter += 1

        shutil.copy2(src_path, dest_path)

        conn = DatabaseConfig.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO APPRENTI_PDF (ID_APP, NOM, CHEMIN)
            VALUES (?, ?, ?)
        """, (apprenti_id, os.path.basename(dest_path), dest_path))
        conn.commit()
        conn.close()
        return True, "Fichier ajouté avec succès"
    except Exception as ex:
        return False, str(ex)


def get_pdfs_apprenti(apprenti_id):
    conn = DatabaseConfig.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            ID,
            NOM,
            CHEMIN,
            CONVERT(VARCHAR, DATE_AJOUT, 103) AS DATE_AJOUT
        FROM APPRENTI_PDF
        WHERE ID_APP = ?
        ORDER BY DATE_AJOUT DESC
    """, (apprenti_id,))
    cols = [col[0] for col in cursor.description]
    rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
    conn.close()
    return rows


def delete_pdf_apprenti(pdf_id):
    try:
        conn = DatabaseConfig.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CHEMIN FROM APPRENTI_PDF WHERE ID = ?", (pdf_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return False, "Fichier introuvable"

        chemin = row[0]
        if os.path.exists(chemin):
            os.remove(chemin)

        cursor.execute("DELETE FROM APPRENTI_PDF WHERE ID = ?", (pdf_id,))
        conn.commit()
        conn.close()
        return True, "Fichier supprimé"
    except Exception as ex:
        return False, str(ex)
def load_sexes() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, type_centre FROM SFAMILIALE")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement sexes : {e}")
        return []
    finally:
        conn.close()

def load_communes():
    conn = DatabaseConfig.get_connection()
    if not conn: return []
    try:    
        cursor = conn.cursor()
        cursor.execute("SELECT ID, lib_commune, code_commune FROM COMMUNES")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur communes: {e}")
        return []
    finally:
        conn.close()

def load_diplome():
    conn = DatabaseConfig.get_connection()
    if not conn: return []
    try:    
        cursor = conn.cursor()
        cursor.execute("SELECT ID, LIB_DIPLOME FROM DIPLOME")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur diplome: {e}")
        return []
    finally:
        conn.close()
def load_niveaux() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, lib_niveau FROM NIVEAU")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement niveaux : {e}")
        return []
    finally:
        conn.close()
def load_niveaux_scolaires() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, NIVEAU_SCOL FROM NIVEAU_SCOLAIRE ORDER BY ID")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement niveaux scolaires : {e}")
        return []
    finally:
        conn.close()
def load_sous_specialites() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ID, ID_SP, code_sp, LIB_SSP
            FROM SPECIALITE
            ORDER BY LIB_SSP
        """)
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement spécialités : {e}")
        return []
    finally:
        conn.close()
def load_specialites() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ID, LIBELLE_SP, CODE_SP
            FROM BRANCHE
            ORDER BY LIBELLE_SP
        """)
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement branches : {e}")
        return []
    finally:
        conn.close()
def load_sous_specialites_by_sp(sp_id) -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ID, LIB_SSP
            FROM SPECIALITE
            WHERE ID_SP = ?
            ORDER BY LIB_SSP
        """, (sp_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement sous-spécialités : {e}")
        return []
    finally:
        conn.close()
def load_nature_moyens() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, TYPE_MY FROM NATUREMOYEN ORDER BY TYPE_MY")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement natures moyens : {e}")
        return []
    finally:
        conn.close()
def load_methode_calcul() -> list :
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID , METHODE_CALCULE FROM MOYEN")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur chargement natures moyens : {e}")
        return []
    finally:
        conn.close()
def load_maitres():
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ID, NOM, PRENOM
            FROM MAITREAPPRENTISSAGE
            ORDER BY NOM
        """)
        return cursor.fetchall()
    except Exception as e:
        print("Erreur MA:", e)
        return []
    finally:
        conn.close()
def load_groupages():
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, groupage FROM GROUPAGE ORDER BY groupage")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur load_groupages: {e}")
        return []
    finally:
        conn.close()
def load_moyens():
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, CODE, DESIGNATION ,ID_NMY FROM MOYEN ORDER BY DESIGNATION")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur load_moyens: {e}")
        return []
    finally:
        conn.close()
def load_moyens_by_nature(nature_id):
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ID, CODE, DESIGNATION
            FROM MOYEN
            WHERE ID_NMY = ?
            ORDER BY DESIGNATION
        """, (nature_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Erreur load_moyens_by_nature: {e}")
        return []
    finally:
        conn.close()   

def open_modifier_affectation_moyen_dialog(page, id_affectation_moyen, on_refresh):
        conn = DatabaseConfig.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT am.ID, am.ID_MY, am.QUANTITE, am.PRIX_U,
                    m.DESIGNATION, m.CODE
                FROM [AFFECTATION MOYEN] am
                INNER JOIN MOYEN m ON m.ID = am.ID_MY
                WHERE am.ID = ?
            """, (id_affectation_moyen,))
            row = cursor.fetchone()
            if not row:
                return

            moyens = load_moyens()
            selected_moyen_id = {"value": str(row[1])}
            moyens_all         = moyens                       
            selected_nature_id = {"value": None}               
            prix_u_field       = ft.TextField(                 
                label="Prix unitaire (DA)", width=180,
                value=str(row[3] or ""),
                border=ft.InputBorder.OUTLINE, border_radius=10,
                visible=True,
            )


            moyen_label = ft.Text(
                f"✅ {row[4]} ({row[5]})", size=12, color="#20398d"
            )
            moyen_list = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=150)
            prix_u_field = ft.TextField(
                label="Prix unitaire (DA)", width=180,
                value=str(row[3] or ""),
                border=ft.InputBorder.OUTLINE, border_radius=10,
                visible=True,
            )
            def build_moyen_list(q=""):
                moyen_list.controls = []

                if not selected_nature_id["value"]:
                    moyen_list.controls = [
                        ft.Text("Sélectionnez une nature d'abord", color="#9CA3AF")
                    ]
                    return

                filtered = [
                    m for m in moyens_all
                    if str(m[0]) 
                ]

                filtered = [
                    m for m in filtered
                    if get_moyen_nature_id(m[0]) == int(selected_nature_id["value"])
                ]

                filtered = [
                    m for m in filtered
                    if f"{m[2]} ({m[1]})".lower().startswith(q.lower())
                ]

                for m in filtered:
                    label  = f"{m[2]} ({m[1]})"
                    is_sel = selected_moyen_id["value"] == str(m[0])

                    prix_actuel = get_prix_moyen(m[0])
                    prix_str    = f"  —  {prix_actuel} DA" if prix_actuel else ""

                    def on_sel(e, mid=str(m[0]), lbl=label, prix=prix_actuel):
                        selected_moyen_id["value"] = mid
                        moyen_label.value = f"✅ {lbl}"
                        moyen_label.color = "#20398d"

                        prix_u_field.value   = str(prix) if prix else ""
                        prix_u_field.visible = True

                        build_moyen_list(search_moyen.value or "")
                        moyen_label.update()
                        moyen_list.update()
                        prix_u_field.update()

                    moyen_list.controls.append(ft.Container(
                        content=ft.Row([
                            ft.Text(label, size=12,
                                    color="#FFFFFF" if is_sel else "#1F2937",
                                    expand=True),
                            ft.Text(prix_str, size=11,
                                    color="#FFFFFF" if is_sel else "#10B981",
                                    italic=True),
                        ]),
                        bgcolor="#20398d" if is_sel else "#F9FAFB",
                        border_radius=8,
                        padding=ft.Padding(10, 6, 10, 6),
                        border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                        on_click=on_sel,
                    ))

            search_moyen = ft.TextField(
                hint_text="Rechercher un moyen...",
                border=ft.InputBorder.OUTLINE, border_radius=10,
                height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
                on_change=lambda e: (build_moyen_list(e.control.value), moyen_list.update()),
            )

            build_moyen_list()

            qte_field  = ft.TextField(
                label="Quantité", width=150,
                value=str(row[2] or 1),
                border=ft.InputBorder.OUTLINE, border_radius=10,
            )

            def save_moyen(e):
                if not selected_moyen_id["value"]:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Sélectionnez un moyen"))
                    page.snack_bar.open = True
                    page.update()
                    return

                if not qte_field.value or not qte_field.value.strip().isdigit():
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Quantité invalide"))
                    page.snack_bar.open = True
                    page.update()
                    return

                conn2 = DatabaseConfig.get_connection()
                if not conn2:
                    return
                try:
                    cursor2 = conn2.cursor()

                    cursor2.execute(
                        "SELECT ID_NMY FROM MOYEN WHERE ID = ?",
                        (int(selected_moyen_id["value"]),)
                    )
                    nmy_row = cursor2.fetchone()
                    id_nmy  = nmy_row[0] if nmy_row else None

                    prix = float(prix_u_field.value) if prix_u_field.value \
                        and prix_u_field.value.strip() else None

                    cursor2.execute("""
                        UPDATE [AFFECTATION MOYEN] SET
                            ID_MY    = ?,
                            ID_NMY   = ?,
                            QUANTITE = ?,
                            PRIX_U   = ?
                        WHERE ID = ?
                    """, (
                        int(selected_moyen_id["value"]),
                        id_nmy,
                        int(qte_field.value),
                        prix,
                        id_affectation_moyen,
                    ))
                    conn2.commit()
                    dialog.open = False
                    page.snack_bar = ft.SnackBar(ft.Text("✅ Moyen modifié avec succès"))
                    page.snack_bar.open = True
                    on_refresh()
                except Exception as ex:
                    page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {str(ex)[:100]}"))
                    page.snack_bar.open = True
                    page.update()
                finally:
                    conn2.close()

            dialog = ft.AlertDialog(
                title=ft.Text("✏️ Modifier le moyen affecté",
                            color="#20398d", weight="bold"),
                content=ft.Container(
                    width=420, height=400,
                    content=ft.Column([
                        ft.Text("Moyen *", size=13, weight="bold", color="#20398d"),
                        search_moyen, moyen_label, moyen_list,
                        ft.Divider(),
                        ft.Row([qte_field, prix_u_field], spacing=10),
                    ], spacing=8, scroll=ft.ScrollMode.AUTO),
                ),
                actions=[
                    ft.TextButton("Annuler",
                                on_click=lambda e: (setattr(dialog, 'open', False), page.update())),
                    ft.ElevatedButton("💾 Enregistrer", bgcolor="#20398d",
                                    on_click=save_moyen),
                ],
                actions_alignment=ft.MainAxisAlignment.END,
            )
            dialog.open = True
            page.dialog = dialog
            page.update()

        except Exception as e:
            print(f"Erreur open_modifier_moyen: {e}")
        finally:
            conn.close()
def load_moyens_apprenti(page, apprenti_id , on_refresh):
    moyens = get_moyens_apprenti(apprenti_id)
    if not moyens:
        return [ft.Text("Aucun moyen affecté", italic=True, color="#9CA3AF", size=12)]

    rows = []
    for m in moyens:
        rows.append(
            ft.Container(
                content=ft.Row([
                    ft.Icon(ft.icons.BUILD_CIRCLE_OUTLINED, color="#20398d", size=20),
                    ft.Column([
                        ft.Text(m.get("DESIGNATION", ""), weight="bold", size=13),
                        ft.Row([
                            ft.Text(f"Code : {m.get('CODE', '')}",
                                    size=11, color="#6B7280"),
                            ft.Text(f"  |  Qté : {m.get('QUANTITE', '')} {m.get('U.M', '')}",
                                    size=11, color="#6B7280"),
                            ft.Text(f"  |  Prix U : {m.get('PRIX_U', '')} DA",
                                    size=11, color="#6B7280"),
                        ]),
                        ft.Text(
                            f"Affecté le : {m.get('DATE_MY', '')}",
                            size=11, color="#10B981"
                        ),
                    ], spacing=2, expand=True),
                    ft.IconButton(
                        ft.icons.EDIT,
                        icon_color="#20398d",
                        tooltip="Modifier",
                        on_click=lambda e, mid=m.get("ID"): open_modifier_affectation_moyen_dialog(
                            page, mid, on_refresh
                        )
                    ),
                ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.START),
                bgcolor="#EFF6FF",
                border_radius=8,
                padding=10,
                border=ft.border.all(1, "#BFDBFE"),
            )
        )
    return rows


def load_pdfs_apprenti(page, apprenti_id, on_refresh):
    pdfs = get_pdfs_apprenti(apprenti_id)
    if not pdfs:
        return [ft.Text("Aucun fichier dans le dossier", italic=True,
                        color="#9CA3AF", size=12)]

    rows = []
    for pdf in pdfs:
        def open_pdf(e, path=pdf.get("CHEMIN", "")):
            import subprocess, sys
            try:
                if sys.platform == "win32":
                    os.startfile(path)
                else:
                    subprocess.Popen(["xdg-open", path])
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"❌ {ex}"))
                page.snack_bar.open = True
                page.update()

        def confirm_delete_pdf(e, pid=pdf.get("ID"), nom=pdf.get("NOM", "")):
            def do_delete(ev):
                dlg.open = False
                success, msg = delete_pdf_apprenti(pid)
                page.snack_bar = ft.SnackBar(
                    ft.Text(f"✅ {msg}" if success else f"❌ {msg}")
                )
                page.snack_bar.open = True
                on_refresh()   

            def cancel(ev):
                dlg.open = False
                page.update()

            dlg = ft.AlertDialog(
                title=ft.Text("Supprimer le fichier ?", color="#EF4444"),
                content=ft.Text(nom),
                actions=[
                    ft.TextButton("Annuler", on_click=cancel),
                    ft.TextButton("Supprimer", on_click=do_delete,
                                  style=ft.ButtonStyle(color="#EF4444")),
                ],
            )
            page.dialog = dlg
            dlg.open = True
            page.update()

        rows.append(
            ft.Container(
                content=ft.Row([
                    ft.Icon(ft.icons.PICTURE_AS_PDF, color="#EF4444", size=22),
                    ft.Column([
                        ft.Text(pdf.get("NOM", ""), weight="bold", size=13),
                        ft.Text(f"Ajouté le : {pdf.get('DATE_AJOUT', '')}",
                                size=11, color="#6B7280"),
                    ], spacing=2, expand=True),
                    ft.IconButton(ft.icons.OPEN_IN_NEW, icon_color="#20398d",
                                  tooltip="Ouvrir", on_click=open_pdf),
                    ft.IconButton(ft.icons.DELETE_OUTLINE, icon_color="#EF4444",
                                  tooltip="Supprimer", on_click=confirm_delete_pdf),
                ], vertical_alignment=ft.CrossAxisAlignment.CENTER),
                bgcolor="#FFF7F7",
                border_radius=8,
                padding=10,
                border=ft.border.all(1, "#FECACA"),
            )
        )
    return rows
def load_maitres_sidebar():
    conn = DatabaseConfig.get_connection()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, NOM, PRENOM FROM MAITREAPPRENTISSAGE ORDER BY NOM")
        return cursor.fetchall()
    except: return []
    finally: conn.close()

def load_projets_sidebar():
    conn = DatabaseConfig.get_connection()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT ID, LIB_PROJET FROM CENTRE_DE_COUT ORDER BY LIB_PROJET")
        return cursor.fetchall()
    except: return []
    finally: conn.close()

def get_apprentis_by_specialite(sp_id: int) -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                A.ID,
                A.NOM,
                A.PRENOM,
                A.MAIL,
                A.TELEPHONE,
                A.photo_path,
                A.DATENAISSANCE,
                A.DATE_D,
                A.DATE_F,
                A.ID_SP,
                A.DUREE,
                A.PERIODE_ESSAI,
                A.LIEUXNAISSANCE,
                A.ADRESSE,
                A.ID_WIL,
                A.ID_COM,
                A.ID_SF,
                A.ESSAI_D,
                A.ESSAI_F,
                A.NINSCRIPT,
                E.DENOMINATION  AS SOCIETE,
                E.STATUT_JURIDIQUE,
                E.TELEPHONE      AS TEL_EMPLOYEUR,
                E.EMAIL          AS EMAIL_EMPLOYEUR,
                S.LIB_SSP        AS SPECIALITE,
                N.num_niveau     AS NIVEAU,
                A.code_app,
                SC.STATUT_CONTRAT AS STATUT_CONTRAT
            FROM APPRENTIE A
            LEFT JOIN EMPLOYEUR      E  ON A.id_employeur  = E.ID
            LEFT JOIN SPECIALITE     S  ON A.ID_SP         = S.ID
            LEFT JOIN NIVEAU         N  ON A.ID_NIV        = N.ID
            LEFT JOIN STATUT_CONTRAT SC ON A.ID_STAT_CONT  = SC.ID
            WHERE A.ID_SP = ?
            ORDER BY A.NOM, A.PRENOM
        """, (sp_id,))

        rows = cursor.fetchall()
        result = []
        for row in rows:
            photo_path = (
                PhotoManager.get_photo_display_path(row[5])
                if row[5]
                else "assets/default_avatar.png"
            )
            sexe = "Masculin" if row[16] == 1 else "Féminin" if row[16] == 2 else ""

            def fmt(d):
                return d.strftime("%d/%m/%Y") if d else ""

            result.append({
                "ID":               row[0],
                "NOM":              row[1]  or "",
                "PRENOM":           row[2]  or "",
                "MAIL":             row[3]  or "",
                "TELEPHONE":        row[4]  or "",
                "photo_path":       photo_path,
                "DATENAISSANCE":    fmt(row[6]),
                "DATE_D":           fmt(row[7]),
                "DATE_F":           fmt(row[8]),
                "ID_SP":            row[9],
                "DUREE":            row[10] or 0,
                "PERIODE_ESSAI":    row[11] or 0,
                "LIEU_NAISSANCE":   row[12] or "",
                "ADRESSE":          row[13] or "",
                "ID_WIL":           row[14],
                "ID_COM":           row[15],
                "SEXE":             sexe,
                "ID_SF":            row[16],
                "ESSAI_D":          fmt(row[17]),
                "ESSAI_F":          fmt(row[18]),
                "NINSCRIPT":        row[19] or "",
                "SOCIETE":          row[20] or "BTPH",
                "STATUT_JURIDIQUE": row[21] or "",
                "TEL_EMPLOYEUR":    row[22] or "",
                "EMAIL_EMPLOYEUR":  row[23] or "",
                "SPECIALITE":       row[24] or "Non spécifiée",
                "NIVEAU":           row[25] or "",
                "code_app":         row[26] or "",
                "statut_contrat":   row[27] or "",
            })

        return result

    except Exception as e:
        print(f"[ERREUR] get_apprentis_by_specialite: {e}")
        return []

    finally:
        conn.close()
def get_apprentis_by_maitre(maitre_id: int) -> list:
    conn = DatabaseConfig.get_connection()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT A.ID, A.NOM, A.PRENOM, A.MAIL, A.TELEPHONE,
                   A.photo_path, A.code_app,
                   SC.STATUT_CONTRAT, S.LIB_SSP
            FROM APPRENTIE A
            LEFT JOIN STATUT_CONTRAT SC ON A.ID_STAT_CONT = SC.ID
            LEFT JOIN SPECIALITE S ON A.ID_SP = S.ID
            WHERE A.ID_MAITRE = ?
            ORDER BY A.NOM, A.PRENOM
        """, (maitre_id,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append({
                "ID": row[0], "NOM": row[1] or "", "PRENOM": row[2] or "",
                "MAIL": row[3] or "", "TELEPHONE": row[4] or "",
                "photo_path": PhotoManager.get_photo_display_path(row[5]) if row[5] else "assets/default_avatar.png",
                "code_app": row[6] or "",
                "statut_contrat": row[7] or "",
                "sous_specialite": row[8] or "",
            })
        return result
    except Exception as e:
        print(f"Erreur get_apprentis_by_maitre: {e}")
        return []
    finally: conn.close()

def get_apprentis_by_projet(projet_id: int) -> list:
    conn = DatabaseConfig.get_connection()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT A.ID, A.NOM, A.PRENOM, A.MAIL, A.TELEPHONE,
                   A.photo_path, A.code_app,
                   SC.STATUT_CONTRAT, S.LIB_SSP
            FROM APPRENTIE A
            INNER JOIN AFFECTATION AF ON AF.ID_APP = A.ID
            LEFT JOIN STATUT_CONTRAT SC ON A.ID_STAT_CONT = SC.ID
            LEFT JOIN SPECIALITE S ON A.ID_SP = S.ID
            WHERE AF.ID_PROJET = ?
            ORDER BY A.NOM, A.PRENOM
        """, (projet_id,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append({
                "ID": row[0], "NOM": row[1] or "", "PRENOM": row[2] or "",
                "MAIL": row[3] or "", "TELEPHONE": row[4] or "",
                "photo_path": PhotoManager.get_photo_display_path(row[5]) if row[5] else "assets/default_avatar.png",
                "code_app": row[6] or "",
                "statut_contrat": row[7] or "",
                "sous_specialite": row[8] or "",
            })
        return result
    except Exception as e:
        print(f"Erreur get_apprentis_by_projet: {e}")
        return []
    finally: conn.close()
def get_niveau_scolaire_lib(id_niv) -> str:
    if not id_niv:
        return ""
    conn = DatabaseConfig.get_connection()
    if not conn:
        return ""
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT NIVEAU_SCOL FROM NIVEAU_SCOLAIRE WHERE ID = ?",
            (id_niv,)
        )
        row = cursor.fetchone()
        return (row[0] or "").strip() if row else ""
    except Exception as e:
        print(f"Erreur niveau scolaire : {e}")
        return ""
    finally:
        conn.close()
def normalize(apprenti):
    return {k.lower(): v for k, v in apprenti.items()}

def statut_color(statut: str) -> str:
    s = (statut or "").strip()
    
    if s == "Ouvert":
        return "#10B981"   
    elif s == "Resilie":
        return "#EF4444"  
    elif s == "Termine":
        return "#3B82F6"   
    else:
        return "#6B7280"  
def _pdf_fix_text(txt):
    if txt is None:
        return ""
    return str(txt).encode("latin-1", "replace").decode("latin-1")

def _pdf_clean_filename(name):
    name = "".join(c for c in name if c.isalnum() or c in (' ', '_', '-', '.')).strip()
    return name[:80]

IMAGE_PATH_PDF = os.path.join(BASE_DIR, "assets", "btphf.png")

def generer_fiche_individuelle(aff_id: int) -> tuple:
    try:
        from fpdf import FPDF

        conn = DatabaseConfig.get_connection()
        if not conn:
            return False, "Erreur connexion BD"
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COALESCE(a.NOM, '_'),
                COALESCE(a.PRENOM, '_'),
                COALESCE(FORMAT(a.DATENAISSANCE, 'dd/MM/yyyy'), '_'),
                COALESCE(a.TELEPHONE, '_'),
                COALESCE(a.ADRESSE, '_'),
                COALESCE(cf.adresse, '_'),
                COALESCE(sp.LIB_SSP, '_'),
                COALESCE(ma.NOM, '_'),
                COALESCE(ma.PRENOM, '_'),
                COALESCE(wil.wilaya, '_'),
                COALESCE(dip.LIB_DIPLOME, '_'),
                COALESCE(cdc.LIB_PROJET, '_'),
                COALESCE(FORMAT(aff.DATE_DR, 'dd/MM/yyyy'), '_'),
                COALESCE(FORMAT(aff.DATE_F,  'dd/MM/yyyy'), '_'),
                COALESCE(st.STATUT_CONTRAT, '_'),
                CAST(COALESCE((SELECT COUNT(*) FROM Pointage p
                               WHERE p.ID_AFFECTATION = ? AND p.status = 'present'), 0)
                     AS NVARCHAR(10))
            FROM AFFECTATION aff
            LEFT JOIN APPRENTIE          a   ON aff.ID_APP     = a.id
            LEFT JOIN CENTRE_DE_COUT     cdc ON aff.ID_PROJET  = cdc.ID
            LEFT JOIN SPECIALITE         sp  ON a.ID_SSP       = sp.ID
            LEFT JOIN MAITREAPPRENTISSAGE ma  ON aff.ID_MA      = ma.ID
            LEFT JOIN WILAYA             wil ON a.ID_WIL       = wil.ID
            LEFT JOIN DIPLOME            dip ON a.ID_DIPL      = dip.ID
            LEFT JOIN CENTRE_FORMATION   cf  ON a.ID_CF        = cf.ID
            LEFT JOIN STATUT_CONTRAT     st  ON a.ID_STAT_CONT = st.ID
            WHERE aff.ID = ?
        """, (aff_id, aff_id))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False, "Aucune donnée trouvée pour cette affectation"

        nom_complet = f"{row[0]}_{row[1]}"
        specialite  = row[6].replace('Non renseigné', '').strip()

        pdf = FPDF()
        pdf.add_page()
        pdf.set_margins(15, 10, 15)

        if os.path.exists(IMAGE_PATH_PDF):
            try:
                pdf.image(IMAGE_PATH_PDF, x=14, y=11, w=45, h=26)
            except:
                pass

        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(80, 80, 80)
        for i, info in enumerate(["R.c: 98B 0022147-22/00", "A.lz 22648611011",
                                   "N.I.F: O99822002214716", "N.I.S: O 98S 2201 00183 46",
                                   "BTPH-HASNAOUI.COM"]):
            pdf.set_xy(165, 16 + (i * 4))
            pdf.cell(30, 3.5, info, 0, 0, "R")

        pdf.set_xy(0, 42)
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(0, 70, 150)
        pdf.cell(0, 8, "GROUPE DES SOCIETES HASNAOUI", 0, 1, "C")
        pdf.cell(0, 8, "BTPH HASNAOUI SPA", 0, 1, "C")
        pdf.set_xy(0, 65)
        pdf.set_font("Helvetica", "B", 20)
        pdf.cell(0, 12, "FICHE DE CHARGE INDIVIDUELLE", 0, 1, "C")

        y_pos = 110
        fields = [
            ("Nom de l'apprentie",           row[0]),
            ("Prenom de l'apprentie",        row[1]),
            ("Date de Naissance",            row[2]),
            ("Telephone",                    row[3]),
            ("Adresse",                      row[4]),
            ("Centre de Formation",          row[5]),
            ("Specialite",                   row[6]),
            ("Nom Maitre Appr.",             row[7]),
            ("Prenom Maitre Apprentissage",  row[8]),
            ("Wilaya",                       row[9]),
            ("Diplome a preparer",           row[10]),
            ("Centre de cout",               row[11]),
            ("Date debut apprentissage",     row[12]),
            ("Date fin apprentissage",       row[13]),
            ("Statut de contrat",            row[14]),
            ("Jours presents",               row[15]),
        ]
        for label, value in fields:
            pdf.set_xy(20, y_pos)
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(60, 7, f"{label}:", 0, 0)
            pdf.set_xy(85, y_pos)
            pdf.set_font("Helvetica", "", 9)
            pdf.cell(105, 7, _pdf_fix_text(value), 0, 0)
            y_pos += 8

        pdf.add_page()
        y_pos = 15
        pdf.set_xy(20, y_pos)
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(0, 70, 150)
        pdf.cell(160, 8, _pdf_fix_text("SUIVI DES PRÉSALAIRES"), 0, 1, "C")
        y_pos += 12

        cur.execute("""
            SELECT Mois, Nb_Jours, presalaire, prime, total
            FROM fiche_de_charge_individuel
            WHERE ID = ?
            ORDER BY CASE Mois
                WHEN 'Janvier' THEN 1 WHEN 'Février' THEN 2 WHEN 'Mars' THEN 3
                WHEN 'Avril' THEN 4 WHEN 'Mai' THEN 5 WHEN 'Juin' THEN 6
                WHEN 'Juillet' THEN 7 WHEN 'Août' THEN 8 WHEN 'Septembre' THEN 9
                WHEN 'Octobre' THEN 10 WHEN 'Novembre' THEN 11 WHEN 'Décembre' THEN 12
                WHEN 'Total annuel' THEN 13 END
        """, (aff_id,))
        rows = cur.fetchall()

        pdf.set_xy(15, y_pos)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_fill_color(200, 220, 255)
        for h in [_pdf_fix_text("Mois"), _pdf_fix_text("Jours Oeuvrable"),
                  _pdf_fix_text("Présalaire"), _pdf_fix_text("Prime"),
                  _pdf_fix_text("Total Mensuelle")]:
            pdf.cell(36, 6, h, 1, 0, "C", 1)
        y_pos += 7

        pdf.set_font("Helvetica", "", 8)
        for row_data in rows:
            mois, jours, presalaire, prime, total = row_data
            if mois == 'Total annuel':
                pdf.set_font("Helvetica", "B", 9)
                pdf.set_fill_color(255, 235, 180)
            else:
                pdf.set_font("Helvetica", "", 8)
                pdf.set_fill_color(255, 255, 255)
            pdf.set_xy(15, y_pos)
            pdf.cell(36, 6, mois[:15], 1, 0, "L", 1)
            pdf.cell(36, 6, f"{int(jours)}", 1, 0, "C", 1)
            pdf.cell(36, 6, f"{int(presalaire)}", 1, 0, "R", 1)
            pdf.cell(36, 6, f"{int(prime)}", 1, 0, "R", 1)
            pdf.cell(36, 6, f"{int(total)}", 1, 0, "R", 1)
            y_pos += 7

        def _page_mensuelle(titre, label_col, view_sql):
            pdf.add_page()
            yp = 15
            pdf.set_xy(20, yp); pdf.set_font("Helvetica", "B", 14)
            pdf.set_text_color(0, 70, 150)
            pdf.cell(160, 8, _pdf_fix_text(titre), 0, 1, "C"); yp += 12
            pdf.set_xy(15, yp); pdf.set_font("Helvetica", "B", 9)
            pdf.set_fill_color(200, 220, 255)
            for h in ["Mois", "Jours", label_col]:
                pdf.cell(60, 6, h, 1, 0, "C", 1)
            yp += 7
            cur.execute(view_sql, (aff_id,))
            data_rows = cur.fetchall()
            pdf.set_font("Helvetica", "", 8)
            cout_j = next((r[2] for r in data_rows if r[2] not in (None, 0)), 0)
            for r in data_rows:
                mois, jours, _, valeur = r
                if mois == 'Total annuel':
                    pdf.set_font("Helvetica", "B", 9); pdf.set_fill_color(255, 235, 180)
                else:
                    pdf.set_font("Helvetica", "", 8); pdf.set_fill_color(255, 255, 255)
                pdf.set_xy(15, yp)
                pdf.cell(60, 6, mois[:15], 1, 0, "L", 1)
                pdf.cell(60, 6, f"{int(jours)}", 1, 0, "C", 1)
                pdf.cell(60, 6, f"{float(valeur):.2f}", 1, 0, "R", 1)
                yp += 7
            yp += 3
            pdf.set_xy(15, yp); pdf.set_font("Helvetica", "B", 9)
            pdf.set_fill_color(240, 240, 240)
            pdf.cell(180, 6,
                     _pdf_fix_text(f"Coût journalier {label_col.lower()} : {float(cout_j):.2f} DA/jour"),
                     1, 0, "C", 1)

        _page_mensuelle("SUIVI PRIME DE PANIER",      "Prime panier",
                        "SELECT Mois, Jours_Mois, prix_journalier, prime_panier FROM vw_prime_panier WHERE ID_AF = ? ORDER BY num_mois")
        _page_mensuelle("SUIVI TRANSPORT MENSUEL",    "Transport",
                        "SELECT Mois, Jours_Mois, prix_journalier, transport FROM vw_transport WHERE ID_AF = ? ORDER BY num_mois")
        _page_mensuelle("SUIVI ASSURANCE ROUTIERE",   _pdf_fix_text("Assurance Routière"),
                        "SELECT Mois, Jours_Mois, prix_journalier, assurance FROM vw_assurance WHERE ID_AF = ?")
        _page_mensuelle("CONSOMMATION GASOIL",        _pdf_fix_text("Répartition Gasoil"),
                        "SELECT Mois, Jours_Mois, prix_journalier, gasoil FROM vw_gasoile WHERE ID_AF = ?")

        def _page_table_simple(titre, query):
            pdf.add_page()
            yp = [15]
            pdf.set_xy(20, yp[0]); pdf.set_font("Helvetica", "B", 12)
            pdf.set_fill_color(200, 220, 255)
            pdf.cell(160, 8, titre, 0, 1, "C"); yp[0] += 10
            pdf.set_xy(10, yp[0]); pdf.set_font("Helvetica", "B", 9)
            pdf.set_fill_color(200, 220, 255)
            headers = ["Désignation", "Code", "U.M", "Qté", "Prix", "Montant"]
            widths  = [80, 30, 15, 15, 25, 25]
            for h, w in zip(headers, widths):
                pdf.cell(w, 6, h, 1, 0, "C", 1)
            yp[0] += 6
            cur.execute(query, (aff_id,))
            data_rows = cur.fetchall()
            pdf.set_font("Helvetica", "", 8)
            total = 0
            for i in range(max(1, len(data_rows))):
                pdf.set_xy(10, yp[0])
                if i < len(data_rows):
                    r = data_rows[i]
                    qte = float(r[3] or 0); prix = float(r[4] or 0)
                    montant = qte * prix; total += montant
                    pdf.cell(widths[0], 6, _pdf_fix_text(str(r[0])[:80]), 1, 0, "L")
                    pdf.cell(widths[1], 6, _pdf_fix_text(str(r[1])), 1, 0, "C")
                    pdf.cell(widths[2], 6, _pdf_fix_text(str(r[2])), 1, 0, "C")
                    pdf.cell(widths[3], 6, f"{qte}", 1, 0, "C")
                    pdf.cell(widths[4], 6, f"{prix:.2f}", 1, 0, "R")
                    pdf.cell(widths[5], 6, f"{montant:.2f}", 1, 0, "R")
                else:
                    for w in widths: pdf.cell(w, 6, "", 1, 0, "C")
                yp[0] += 6
            pdf.set_xy(10, yp[0]); pdf.set_font("Helvetica", "B", 9)
            pdf.set_fill_color(255, 235, 180)
            pdf.cell(sum(widths[:-1]), 6, "Le coût total :", 1, 0, "C", 1)
            pdf.cell(widths[-1], 6, f"{total:.2f}", 1, 0, "R", 1)

        _page_table_simple("CONSOMMATION EPI",
            "SELECT DESIGNATION, CODE, UM, QUANTITE, Prix_TTC, Montant_TTC FROM vw_epi WHERE ID_AF = ?")
        _page_table_simple("CONSOMMATION OUTILLAGE",
            "SELECT DESIGNATION, CODE, UM, QUANTITE, PRIX_TTC, Montant_TTC FROM vw_outillage WHERE ID_AF = ?")
        _page_table_simple("CONSOMMATION MATIERE",
            "SELECT DESIGNATION, CODE, UM, QUANTITE, PRIX_TTC, Montant_TTC FROM vw_matiere WHERE ID_AF = ?")
        _page_table_simple("CONSOMMATION FOURNITURE",
            "SELECT DESIGNATION, CODE, UM, QUANTITE, PRIX_TTC, Montant_TTC FROM vw_fourniture WHERE ID_AF = ?")
        _page_table_simple("AMORTISSEMENT IMMOBILISATION",
            "SELECT DESIGNATION, CODE, 'Jour', Jours_Presents, Dotation_Jour, Montant FROM vw_immobilisation WHERE ID_AF = ?")

        def _sum_view(view, field, exclude_total=False):
            q = f"SELECT SUM({field}) FROM {view} WHERE ID_AF = ?"
            if exclude_total:
                q += " AND Mois != 'Total annuel'"
            cur.execute(q, (aff_id,))
            res = cur.fetchone()[0]
            return float(res) if res else 0.0

        charge_presalaire = next((float(r[4]) for r in rows if r[0] == 'Total annuel'), 0.0)
        charge_transport  = _sum_view("vw_transport",    "transport",   True)
        charge_panier     = _sum_view("vw_prime_panier", "prime_panier",True)
        charge_assurance  = _sum_view("vw_assurance",    "assurance",   True)
        charge_gasoil     = _sum_view("vw_gasoile",      "gasoil",      True)
        charge_epi        = _sum_view("vw_epi",          "Montant_TTC")
        charge_outillage  = _sum_view("vw_outillage",    "Montant_TTC")
        charge_matiere    = _sum_view("vw_matiere",      "Montant_TTC")
        charge_fourniture = _sum_view("vw_fourniture",   "Montant_TTC")
        charge_immo       = _sum_view("vw_immobilisation","Montant")
        total_general = (charge_presalaire + charge_transport + charge_panier +
                         charge_assurance + charge_gasoil + charge_epi +
                         charge_outillage + charge_matiere + charge_fourniture + charge_immo)

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(0, 70, 150)
        pdf.cell(0, 10, _pdf_fix_text("TABLEAU RECAPITULATIF DES CHARGES"), 0, 1, "C")
        y = 35
        pdf.set_xy(15, y); pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(200, 220, 255)
        pdf.cell(120, 6, _pdf_fix_text("Catégorie de charge"), 1, 0, "C", 1)
        pdf.cell(60,  6, "Montant (DA)", 1, 1, "C", 1)
        pdf.set_font("Helvetica", "", 10)
        for label, montant in [
            (_pdf_fix_text("Charge 01 : Présalaire mensuel"),         charge_presalaire),
            (_pdf_fix_text("Charge 02 : Coûts de transport"),         charge_transport),
            (_pdf_fix_text("Charge 03 : Prime de panier"),            charge_panier),
            (_pdf_fix_text("Charge 04 : Frais d'assurance routière"), charge_assurance),
            (_pdf_fix_text("Charge 05 : Coût consommation gasoil"),   charge_gasoil),
            (_pdf_fix_text("Charge 06 : Dotation des EPI"),           charge_epi),
            (_pdf_fix_text("Charge 07 : Débitage outillage attribué"),charge_outillage),
            (_pdf_fix_text("Charge 08 : Débit de matière consommée"), charge_matiere),
            (_pdf_fix_text("Charge 09 : Coût Fourniture de bureau"),  charge_fourniture),
            (_pdf_fix_text("Charge 10 : Amortissements immobilisations"), charge_immo),
        ]:
            pdf.set_x(15)
            pdf.cell(120, 6, label, 1, 0, "L")
            pdf.cell(60,  6, f"{montant:.2f}", 1, 1, "R")
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_fill_color(255, 235, 180)
        pdf.set_x(15)
        pdf.cell(120, 8, "TOTAL GÉNÉRAL DES CHARGES :", 1, 0, "L", 1)
        pdf.cell(60,  8, f"{total_general:.2f}", 1, 1, "R", 1)

        sp_clean  = _pdf_clean_filename(specialite or "Sans_specialite")
        base_dir  = os.path.join(os.getcwd(), "---------Fichiers Individuelles---------")
        dossier   = os.path.join(base_dir, sp_clean)
        os.makedirs(dossier, exist_ok=True)
        nom_file  = _pdf_clean_filename(nom_complet)
        filename  = os.path.join(dossier, f"fiche_charge_{nom_file}.pdf")
        try:
            pdf.output(filename)
        except PermissionError:
            filename = os.path.join(dossier, f"fiche_charge_{nom_file}_{int(time.time())}.pdf")
            pdf.output(filename)

        conn.close()
        return True, filename

    except Exception as e:
        return False, str(e)



def generer_fiche_globale(annee: int, id_specialite: int, nom_specialite: str) -> tuple:
    try:
        from fpdf import FPDF

        conn = DatabaseConfig.get_connection()
        if not conn:
            return False, "Erreur connexion BD"
        cur = conn.cursor()

        def clean_text(text):
            if text is None: return ""
            return str(text).encode("latin-1", "replace").decode("latin-1")

        def draw_table_simple_g(pdf, titre, query):
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 14)
            pdf.cell(0, 10, titre, 0, 1, "C"); pdf.ln(5)
            headers = ["Désignation", "Code", "UM", "Qté", "Prix", "Montant"]
            widths  = [70, 25, 20, 15, 30, 30]
            pdf.set_fill_color(200, 220, 255); pdf.set_x(10)
            pdf.set_font("Helvetica", "B", 8)
            for h, w in zip(headers, widths): pdf.cell(w, 8, h, 1, 0, "C", 1)
            pdf.ln(); pdf.set_font("Helvetica", "", 8)
            cur.execute(query, (annee, id_specialite))
            all_rows = cur.fetchall()
            total = 0
            for i in range(max(1, len(all_rows))):
                pdf.set_fill_color(255, 255, 255); pdf.set_x(10)
                if i < len(all_rows):
                    r = all_rows[i]
                    for j, w in enumerate(widths): pdf.cell(w, 6, clean_text(r[j]), 1, 0, "C", 1)
                    try: total += float(r[5])
                    except: pass
                else:
                    for w in widths: pdf.cell(w, 6, "", 1, 0, "C", 1)
                pdf.ln(); pdf.set_x(10)
            pdf.set_font("Helvetica", "B", 9); pdf.set_fill_color(255, 235, 180)
            pdf.cell(sum(widths[:-1]), 8, "TOTAL :", 1, 0, "C", 1)
            pdf.cell(widths[-1], 8, str(int(total)), 1, 1, "R", 1)

        def draw_table_immobilis_g(pdf, titre, query):
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 14)
            pdf.cell(0, 10, titre, 0, 1, "C"); pdf.ln(5)
            headers = ["Désignation", "Code", "Dotation Mensuelle", "Dotation par Jour", "Jours", "Montant"]
            widths  = [70, 25, 30, 30, 15, 25]
            pdf.set_fill_color(200, 220, 255); pdf.set_x(10)
            pdf.set_font("Helvetica", "B", 8)
            for h, w in zip(headers, widths): pdf.cell(w, 8, h, 1, 0, "C", 1)
            pdf.ln(); pdf.set_font("Helvetica", "", 8)
            cur.execute(query, (annee, id_specialite))
            all_rows = cur.fetchall()
            total = 0
            for i in range(max(1, len(all_rows))):
                pdf.set_fill_color(255, 255, 255); pdf.set_x(10)
                if i < len(all_rows):
                    r = all_rows[i]
                    for j, w in enumerate(widths): pdf.cell(w, 6, clean_text(r[j])[:30], 1, 0, "C", 1)
                    try: total += float(r[5])
                    except: pass
                else:
                    for w in widths: pdf.cell(w, 6, "", 1, 0, "C", 1)
                pdf.ln(); pdf.set_x(10)
            pdf.set_font("Helvetica", "B", 9); pdf.set_fill_color(255, 235, 180)
            pdf.cell(sum(widths[:-1]), 8, "TOTAL :", 1, 0, "C", 1)
            pdf.cell(widths[-1], 8, str(int(total)), 1, 1, "R", 1)

        def draw_table_mensuelle_g(pdf, rows_data, titre, label_col):
            pdf.add_page(); yp = 15
            pdf.set_xy(20, yp); pdf.set_font("Helvetica", "B", 14)
            pdf.set_text_color(0, 70, 150)
            pdf.cell(160, 8, titre, 0, 1, "C"); yp += 12
            pdf.set_xy(15, yp); pdf.set_font("Helvetica", "B", 9)
            pdf.set_fill_color(200, 220, 255)
            for h in ["Mois", "Jours", label_col]: pdf.cell(60, 6, h, 1, 0, "C", 1)
            yp += 7; pdf.set_font("Helvetica", "", 8)
            for r in rows_data:
                mois, jours, valeur = r[0], r[1], r[3]
                if mois == "Total annuel":
                    pdf.set_font("Helvetica", "B", 9); pdf.set_fill_color(255, 235, 180)
                else:
                    pdf.set_font("Helvetica", "", 8); pdf.set_fill_color(255, 255, 255)
                pdf.set_xy(15, yp)
                pdf.cell(60, 6, str(mois)[:15], 1, 0, "L", 1)
                pdf.cell(60, 6, str(jours), 1, 0, "C", 1)
                pdf.cell(60, 6, f"{float(valeur):.2f}", 1, 0, "R", 1)
                yp += 7

        pdf = FPDF()
        pdf.set_margins(15, 10, 15)
        pdf.add_page()
        if os.path.exists(IMAGE_PATH_PDF):
            try: pdf.image(IMAGE_PATH_PDF, x=14, y=11, w=45, h=26)
            except: pass
        pdf.set_font("Helvetica", "", 8); pdf.set_text_color(80, 80, 80)
        for i, info in enumerate(["R.c: 98B 0022147-22/00", "A.lz 22648611011",
                                   "N.I.F: O99822002214716", "N.I.S: O 98S 2201 00183 46",
                                   "BTPH-HASNAOUI.COM"]):
            pdf.set_xy(165, 16 + (i * 4)); pdf.cell(30, 3.5, info, 0, 0, "R")
        pdf.set_xy(0, 42); pdf.set_font("Helvetica", "B", 14); pdf.set_text_color(0, 70, 150)
        pdf.cell(0, 8, "GROUPE DES SOCIETES HASNAOUI", 0, 1, "C")
        pdf.cell(0, 8, "BTPH HASNAOUI SPA", 0, 1, "C")
        pdf.set_xy(0, 65); pdf.set_font("Helvetica", "B", 20)
        pdf.cell(0, 10, "FICHE GLOBALE", 0, 1, "C")

        cur.execute("EXEC dbo.fiche_de_charge_global_filtre @Annee = ?, @ID_SSP = ?",
                    (annee, id_specialite))
        stats_row = cur.fetchone()
        if not stats_row:
            conn.close()
            return False, f"Aucune donnée pour {nom_specialite} - {annee}"
        columns = [c[0] for c in cur.description]
        stats = dict(zip(columns, stats_row))

        pdf.ln(12); pdf.set_font("Helvetica", "", 10); pdf.set_text_color(0, 70, 150)
        y_pos = 90
        for label, key in [
            ("Spécialité", "LIB_SSP"), ("Projet", "LIB_PROJET"),
            ("Date Début", "Date Début"), ("Date Fin", "Date fin"),
            ("Nombre inscrits", "Nombre_Inscrits"),
            ("Contrats ouverts en 31 Décembre", "Nombre_ouvert"),
            ("Contrats terminés", "Nombre_termines"),
            ("Résiliations", "Nombre_Resiliations"), ("Abandons", "Nombre_Abandons"),
            ("Embauchés", "Nombre_Embauches"), ("Total", "Nombre_Global"),
            ("Jours Globale", "Jours_Globale"),
        ]:
            pdf.set_xy(20, y_pos); pdf.set_font("Helvetica", "B", 10)
            pdf.cell(70, 7, f"{label} :", 0, 0)
            pdf.set_xy(90, y_pos); pdf.set_font("Helvetica", "", 10)
            pdf.cell(100, 7, clean_text(stats.get(key, "-")), 0, 0)
            y_pos += 8

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 16); pdf.cell(0, 10, "LISTE DES APPRENTIS", 0, 1, "C"); pdf.ln(5)
        cur.execute("EXEC dbo.fiche_de_charge_global_filtre @Annee = ?, @ID_SSP = ?",
                    (annee, id_specialite))
        row2 = cur.fetchone()
        liste = row2.Liste_Apprentis.split("\n") if row2 and row2.Liste_Apprentis else []
        headers = ["N°", "Nom", "Diplome", "Niveau", "Wilaya", "Statut"]
        widths  = [10, 50, 30, 25, 35, 30]
        pdf.set_fill_color(200, 220, 255); pdf.set_text_color(0, 70, 150); pdf.set_x(10)
        pdf.set_font("Helvetica", "B", 9)
        for h, w in zip(headers, widths): pdf.cell(w, 8, h, 1, 0, "C", 1)
        pdf.ln(); pdf.set_font("Helvetica", "", 8)
        apprentis_ordonnes = []
        for i, ligne in enumerate(liste, 1):
            parts = ligne.split("|")
            if len(parts) < 5: continue
            nom     = parts[0].strip()
            diplome = parts[1].replace("Diplôme:", "").strip()
            niveau  = parts[2].replace("Niveau:", "").strip()
            wilaya  = parts[3].replace("Wilaya:", "").strip()
            statut  = parts[4].replace("Statut:", "").strip()
            apprentis_ordonnes.append(nom)
            pdf.set_fill_color(255, 235, 180); pdf.set_x(10)
            pdf.cell(widths[0], 6, str(i), 1, 0, "C", 1)
            pdf.cell(widths[1], 6, nom[:25], 1)
            pdf.cell(widths[2], 6, diplome[:15], 1)
            pdf.cell(widths[3], 6, niveau[:15], 1)
            pdf.cell(widths[4], 6, wilaya[:15], 1)
            pdf.cell(widths[5], 6, statut[:15], 1)
            pdf.ln()

        pdf.add_page()
        pdf.set_xy(0, 20); pdf.set_font("Helvetica", "B", 14); pdf.set_text_color(0, 70, 150)
        pdf.cell(0, 10, "NOMBRE DES JOURS PAR MOIS", 0, 1, "C"); pdf.ln(5)
        cur.execute("EXEC dbo.Get_Jours_Par_Mois_Par_Apprenti @Annee = ?, @ID_SSP = ?",
                    (annee, id_specialite))
        jours_rows = cur.fetchall()
        table = {}
        mois_list = ["Janvier","Février","Mars","Avril","Mai","Juin",
                     "Juillet","Août","Septembre","Octobre","Novembre","Décembre"]
        for r in jours_rows:
            nom = r[0]; mois_data = r[1:-1]; tot = r[-1]
            table[nom] = dict(zip(mois_list, mois_data)); table[nom]["Total"] = tot
        page_width = pdf.w - 30
        col_nom = 12; col_mois = (page_width - col_nom) / 13
        pdf.set_font("Helvetica", "B", 8); pdf.set_fill_color(200, 220, 255)
        pdf.set_text_color(0, 70, 150); pdf.set_x(10)
        pdf.cell(col_nom, 8, "N°", 1, 0, "C", 1)
        for m in mois_list: pdf.cell(col_mois, 8, m[:3], 1, 0, "C", 1)
        pdf.cell(col_mois, 8, "Total", 1, 1, "C", 1)
        pdf.set_font("Helvetica", "", 7); total_global = 0
        for i, nom in enumerate(apprentis_ordonnes, 1):
            if nom not in table: continue
            pdf.set_x(10)
            pdf.set_fill_color(255, 235, 180); pdf.set_text_color(0, 70, 150)
            pdf.cell(col_nom, 6, str(i), 1, 0, "C", 1)
            tot = 0; pdf.set_fill_color(255, 255, 255)
            for m in mois_list:
                v = int(table[nom].get(m, 0)); tot += v
                pdf.cell(col_mois, 6, str(v), 1, 0, "C", 1)
            total_global += tot
            pdf.cell(col_mois, 6, str(tot), 1, 1, "C", 1)
        pdf.set_font("Helvetica", "B", 10); pdf.set_fill_color(255, 235, 180); pdf.set_x(10)
        pdf.cell(col_nom + len(mois_list) * col_mois, 6, "Jours globale :", 1, 0, "L", 1)
        pdf.cell(col_mois, 6, str(total_global), 1, 1, "R", 1)

        cur.execute("EXEC dbo.sp_transport_par_annee_specialite @Annee = ?, @ID_SSP = ?", (annee, id_specialite))
        rows_transport = cur.fetchall()
        cur.execute("EXEC dbo.sp_prime_de_panier_par_mois_annee_specialite @Annee = ?, @ID_SSP = ?", (annee, id_specialite))
        rows_panier = cur.fetchall()
        cur.execute("EXEC dbo.sp_assurance_par_mois_annee_specialite @Annee = ?, @ID_SSP = ?", (annee, id_specialite))
        rows_assurance = cur.fetchall()
        cur.execute("EXEC dbo.sp_gasoile_par_mois_annee_specialite @Annee = ?, @ID_SSP = ?", (annee, id_specialite))
        rows_gasoil = cur.fetchall()

        draw_table_mensuelle_g(pdf, rows_transport, "SUIVI TRANSPORT",   "Transport")
        draw_table_mensuelle_g(pdf, rows_panier,    "PRIME PANIER",      "Prime")
        draw_table_mensuelle_g(pdf, rows_assurance, "SUIVI ASSURANCE",   "Assurance")
        draw_table_mensuelle_g(pdf, rows_gasoil,    "SUIVI GASOIL",      "Gasoil")

        draw_table_simple_g(pdf, "EPI GLOBAL",
            "EXEC dbo.epi_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?")
        draw_table_simple_g(pdf, "OUTILLAGE GLOBAL",
            "EXEC dbo.outillage_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?")
        draw_table_simple_g(pdf, "MATIERE GLOBAL",
            "EXEC dbo.matiere_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?")
        draw_table_simple_g(pdf, "FOURNITURE GLOBAL",
            "EXEC dbo.fourniture_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?")
        draw_table_immobilis_g(pdf, "AMORTISSEMENT IMMOBILISATION GLOBAL",
            "EXEC dbo.immobilisation_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?")

        cur.execute("EXEC dbo.fiche_de_charge_globale2 @Annee = ?, @ID_SSP = ?", (annee, id_specialite))
        rows_pre = cur.fetchall()
        charge_presalaire = next((float(r[4]) for r in rows_pre if r[0] == 'Total annuel'), 0.0)

        def get_total_exec(proc_sql, field_index):
            try:
                cur.execute(proc_sql, (annee, id_specialite))
                return sum(float(r[field_index]) for r in cur.fetchall()
                           if r[field_index] is not None)
            except: return 0.0

        charge_transport  = get_total_exec("EXEC dbo.sp_transport_par_annee_specialite @Annee = ?, @ID_SSP = ?", 3)
        charge_panier     = get_total_exec("EXEC dbo.sp_prime_de_panier_par_mois_annee_specialite @Annee = ?, @ID_SSP = ?", 3)
        charge_assurance  = get_total_exec("EXEC dbo.sp_assurance_par_mois_annee_specialite @Annee = ?, @ID_SSP = ?", 3)
        charge_gasoil     = get_total_exec("EXEC dbo.sp_gasoile_par_mois_annee_specialite @Annee = ?, @ID_SSP = ?", 3)
        charge_epi        = get_total_exec("EXEC dbo.epi_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?", 5)
        charge_outillage  = get_total_exec("EXEC dbo.outillage_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?", 5)
        charge_matiere    = get_total_exec("EXEC dbo.matiere_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?", 5)
        charge_fourniture = get_total_exec("EXEC dbo.fourniture_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?", 5)
        charge_immo       = get_total_exec("EXEC dbo.immobilisation_globale_par_annee_specialite @Annee = ?, @ID_SSP = ?", 5)
        total_general = (charge_presalaire + charge_transport + charge_panier +
                         charge_assurance + charge_gasoil + charge_epi +
                         charge_outillage + charge_matiere + charge_fourniture + charge_immo)

        pdf.add_page(); y = 20
        pdf.set_font("Helvetica", "B", 14); pdf.set_text_color(0, 70, 150)
        pdf.cell(0, 10, "TABLEAU RECAPITULATIF DES CHARGES", 0, 1, "C")
        y += 15; pdf.set_xy(15, y); pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(200, 220, 255)
        pdf.cell(120, 6, "Catégorie de charge", 1, 0, "C", 1)
        pdf.cell(60,  6, "Montant (DA)", 1, 1, "C", 1)
        pdf.set_font("Helvetica", "", 10)
        for label, montant in [
            ("Charge 01 : Présalaire mensuel",         charge_presalaire),
            ("Charge 02 : Coûts de transport",         charge_transport),
            ("Charge 03 : Prime de panier",            charge_panier),
            ("Charge 04 : Frais d'assurance routière", charge_assurance),
            ("Charge 05 : Coût consommation gasoil",   charge_gasoil),
            ("Charge 06 : Dotation des EPI",           charge_epi),
            ("Charge 07 : Débitage outillage attribué",charge_outillage),
            ("Charge 08 : Débit de matière consommée", charge_matiere),
            ("Charge 09 : Coût Fourniture de bureau",  charge_fourniture),
            ("Charge 10 : Amortissements immobilisations", charge_immo),
        ]:
            pdf.set_x(15)
            pdf.cell(120, 6, label, 1, 0, "L")
            pdf.cell(60,  6, f"{montant:.2f}", 1, 1, "R")
        pdf.set_font("Helvetica", "B", 12); pdf.set_fill_color(255, 235, 180); pdf.set_x(15)
        pdf.cell(120, 8, "TOTAL GÉNÉRAL DES CHARGES :", 1, 0, "L", 1)
        pdf.cell(60,  8, f"{total_general:.2f}", 1, 1, "R", 1)

        base_dir = os.path.join(os.getcwd(), "---------Fichiers Globales---------")
        dossier  = os.path.join(base_dir, str(annee), _pdf_clean_filename(nom_specialite))
        os.makedirs(dossier, exist_ok=True)
        filename = os.path.join(dossier, f"fiche_globale_{_pdf_clean_filename(nom_specialite)}_{annee}.pdf")
        try:
            pdf.output(filename)
        except PermissionError:
            filename = os.path.join(dossier, f"fiche_globale_{_pdf_clean_filename(nom_specialite)}_{annee}_{int(time.time())}.pdf")
            pdf.output(filename)

        conn.close()
        return True, filename

    except Exception as e:
        return False, str(e) 
def get_prix_moyen(moyen_id) -> Optional[float]:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DPA_TTC FROM MOYEN WHERE ID = ?", (moyen_id,))
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] else None
    except:
        return None
    finally:
        conn.close()
class ApprentiForm:
    def __init__(self, page: ft.Page, on_saved=None, apprenti_id=None, current_user=None, on_close=None , edit_mode=False):
        self.page = page
        self.root = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
        self.on_saved = on_saved
        self.apprenti_id = apprenti_id
        self.current_user = current_user
        self.on_close = on_close
        self.edit_mode = edit_mode
        self._original = {}
        self.apprenti_id_db = None
        self.employeur_id_db = None
        self.photo_path = None
        self.photo_file_path = None
        communes_data = load_communes()
        niveaux_data  = load_niveaux_scolaires()
        diplome_data = load_diplome()
        self.data = {}
        if edit_mode and apprenti_id:
            self.data = get_apprenti_complet(apprenti_id)
        self.datenaissance_picker = ft.DatePicker(on_change=self.on_date_naissance_change)
        self.date_debut_picker = ft.DatePicker(on_change=self.on_date_debut_change)
        self.date_fin_picker = ft.DatePicker(on_change=self.on_date_fin_change)
        self.page.overlay.append(self.datenaissance_picker)
        self.page.overlay.append(self.date_debut_picker)
        self.page.overlay.append(self.date_fin_picker)
        emp = get_employeur_default()
        self.employeur_id_db = emp.get("id") or None

        self.employeur_type = ft.RadioGroup(
            value=emp.get("nat_employeur", "Privé"),
            content=ft.Row([
                ft.Radio(value="Public", label="Public"),
                ft.Radio(value="Privé", label="Privé"),
            ], spacing=20)
        )
        self.employeur_denomination = ft.TextField(
            label="Dénomination de l'employeur *", 
            value=emp.get("denomination", ""),
            width=500
        )
        self.employeur_statut_juridique = ft.TextField(
            label="Statut juridique", 
            value=emp.get("statut_juridique", ""),
            width=500
        )
        self.employeur_adresse = ft.TextField(
            label="Adresse", 
            value=emp.get("adresse", ""),
            width=500
        )
        (self.emp_wc_widget,  self.emp_wc,  self.employeur_code_postal) = \
            self.make_wilaya_commune_widget(communes_data)

        self.employeur_telephone = ft.TextField(
            label="Téléphone", 
            value=emp.get("telephone", ""),
            width=200
        )
        self.employeur_fax = ft.TextField(
            label="Fax", 
            value=emp.get("fax", ""),
            width=200
        )
        self.employeur_email = ft.TextField(
            label="E-mail", 
            value=emp.get("email", ""),
            width=300
        )

        self.employeur_id_db = emp.get("id")        
        self.apprenti_nom = ft.TextField(label="Nom *", width=250)
        self.apprenti_prenom = ft.TextField(label="Prénom *", width=250)
        
        self.apprenti_date_naissance = ft.TextField(
            label="Date de naissance *",
            width=150,
            read_only=True,
            value="",
            suffix_icon=ft.icons.CALENDAR_TODAY,
        )
        
        self.apprenti_lieu_naissance = ft.TextField(label="Lieu de naissance", width=250)
        
        sexes = load_sexes()
        self.apprenti_sexe = ft.RadioGroup(
            content=ft.Row([
                ft.Radio(value=str(id_), label=label)
                for id_, label in sexes
            ])
        )
        groupages_data = load_groupages()
        self.apprenti_groupage = ft.Dropdown(
            label="Groupage",
            width=300,
            options=[
                ft.dropdown.Option(key=str(g[0]), text=g[1])
                for g in groupages_data
            ]
        )
        
        self.file_picker = ft.FilePicker(on_result=self.on_photo_result)
        self.page.overlay.append(self.file_picker)
        
        self.photo_image = ft.Container(
            width=150, 
            height=180, 
            bgcolor="#E5E7EB",
            border_radius=ft.border_radius.all(8),
            clip_behavior=ft.ClipBehavior.ANTI_ALIAS, 
            alignment=ft.alignment.center,
            content=ft.Column(
                [
                    ft.Icon(ft.icons.PHOTO_CAMERA, size=50, color="#9CA3AF"),
                    ft.Text("Aucune photo", size=10, color="#6B7280"),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            ),
        )
        
        self.btn_select_photo = ft.ElevatedButton(
            "Importer photo",
            icon=ft.icons.CAMERA_ALT,
            width=150,
            on_click=lambda e: self.file_picker.pick_files(allowed_extensions=["jpg", "jpeg", "png"])
        )
        
        self.photo_status = ft.Text("Aucune photo", size=11, color="gray")

        
        self.apprenti_adresse = ft.TextField(label="Adresse", width=500)
    
        (self.app_wc_widget,  self.app_wc,  self.apprenti_code_postal)  = \
            self.make_wilaya_commune_widget(communes_data)
        self.apprenti_telephone = ft.TextField(label="Téléphone", width=200)
        self.apprenti_email = ft.TextField(label="E-mail *", width=300)
        self.apprenti_niveau_scolaire = ft.Dropdown(
            label="Niveau scolaire",
            width=300,
            options=[
                ft.dropdown.Option(key=str(n[0]), text=n[1])
                for n in niveaux_data
            ]
        )
                
        self.tuteur_nom = ft.TextField(label="Nom", width=250)
        self.tuteur_prenom = ft.TextField(label="Prénom", width=250)
        self.tuteur_adresse = ft.TextField(label="Adresse", width=500)
        (self.tut_wc_widget, self.tut_wc, self.tuteur_code_postal) = \
            self.make_wilaya_commune_widget(communes_data)
        self.tuteur_telephone = ft.TextField(label="Téléphone", width=200)
        self.tuteur_email = ft.TextField(label="E-mail", width=300)
        
        self.formation_denomination = ft.Dropdown(
            label="Dénomination",
            width=300,
            options=[
                ft.dropdown.Option("CFPA"),
                ft.dropdown.Option("INSFP"),
                ft.dropdown.Option("Autre"),
            ]
        )
        
        self.formation_adresse = ft.TextField(label="Adresse", width=500)
        (self.form_wc_widget, self.form_wc, self.formation_code_postal) = \
            self.make_wilaya_commune_widget(communes_data)
  
        self.formation_telephone = ft.TextField(label="Téléphone", width=200)
        self.formation_fax = ft.TextField(label="Fax", width=200)
        self.formation_email = ft.TextField(label="E-mail", width=300)
        
        
        self.formation_code = ft.TextField(
            label="Code",
            width=150,
            read_only=True
        )        
        self.specialites_data = load_sous_specialites()

        self.specialites_map = {
            str(row[0]): {        
                "code": row[2],   
                "id_sp": row[1],  
            }
            for row in self.specialites_data
        }
        self.formation_specialite = ft.Dropdown(
            label="Spécialité / métier *",
            width=400,
            options=[
                ft.dropdown.Option(
                    key=str(row[0]),   
                    text=row[3]       
                )
                for row in self.specialites_data
            ],
            on_change=self.on_specialite_change
        ) 
        self.formation_sous_specialite = ft.Dropdown(
            label="Sous-spécialité *",
            width=400,
            options=[]
        )       
        self.formation_numero_inscription = ft.TextField(label="N° d'inscription", width=200)
        self.formation_duree = ft.TextField(label="Durée (mois)", width=150, value="12")
        
        self.formation_date_debut = ft.TextField(
            label="du (date de début) *",
            width=150,
            read_only=True,
            value="",
            suffix_icon=ft.icons.CALENDAR_TODAY,
        )
        
        fin_date = datetime.now() + timedelta(days=365)
        self.formation_date_fin = ft.TextField(
            label="au (date de fin) *",
            width=150,
            read_only=True,
            value=fin_date.strftime("%d/%m/%Y"),
            suffix_icon=ft.icons.CALENDAR_TODAY,
        )
        
        self.formation_diplome = ft.Dropdown(
            label="Diplôme",
            width=400,
            options=[
                ft.dropdown.Option(key=str(d[0]), text=d[1])
                for d in diplome_data
            ]
        )

        self.formation_periode_essai = ft.TextField(label="Période d'essai (jours)", width=150, value="30")
        self.formation_essai_debut = ft.TextField(label="du", width=150, read_only=True, value="", suffix_icon=ft.icons.CALENDAR_TODAY)
        self.formation_essai_fin = ft.TextField(label="au", width=150, read_only=True, value="", suffix_icon=ft.icons.CALENDAR_TODAY)
        self.essai_debut_picker = ft.DatePicker(on_change=self.on_essai_debut_change)
        self.essai_fin_picker = ft.DatePicker(on_change=self.on_essai_fin_change)
        self.page.overlay.append(self.essai_debut_picker)
        self.page.overlay.append(self.essai_fin_picker)
        self.date_resiliation = ft.TextField(
            label="Date résiliation",
            width=150,
            read_only=True,
            value="",
            suffix_icon=ft.icons.CALENDAR_TODAY,
        )

        self.motif_resiliation = ft.TextField(
            label="Motif résiliation",
            width=400
        )       
                    # Ajouter le FilePicker pour l'OCR
        self.ocr_file_picker = ft.FilePicker(on_result=self.on_ocr_file_result)
        self.page.overlay.append(self.ocr_file_picker)
    
    
        self.btn_save = ft.ElevatedButton("✅ Enregistrer", bgcolor="#20398d", color="white", on_click=self.save)
        self.btn_cancel = ft.OutlinedButton("❌ Annuler", on_click=self.close)


    def on_specialite_change(self, e):
        ssp_id = self.formation_specialite.value

        if not ssp_id or ssp_id not in self.specialites_map:
            self.formation_code.value = ""
            return

        data = self.specialites_map[ssp_id]

        self.formation_code.value = data["code"]
        self.selected_id_sp = data["id_sp"]   

        self.page.update()
    def on_photo_result(self, e):
        if e.files:
            file_path = e.files[0].path
            self.photo_file_path = file_path
            is_valid, message = PhotoManager.is_valid_image(file_path)
            print(f"Valide : {is_valid} — {message}")

            if is_valid:
                try:
                    image_widget = ft.Image(
                        src=file_path,
                        width=150,
                        height=180,
                        fit=ft.ImageFit.COVER,
                        border_radius=ft.border_radius.all(8),
                    )
                    self.photo_image.content = image_widget
                    self.photo_image.border_radius = ft.border_radius.all(8)
                    self.photo_image.clip_behavior = ft.ClipBehavior.ANTI_ALIAS
                    
                    self.photo_status.value = f"✅ {os.path.basename(file_path)}"
                    self.photo_status.color = "green"
                    self.page.update()
                    
                except Exception as ex:
                    self.show_snackbar(f"❌ Erreur: {ex}")
            else:
                self.show_snackbar(f"❌ Photo invalide: {message}")
    
    def on_date_naissance_change(self, e):
        if self.datenaissance_picker.value:
            self.apprenti_date_naissance.value = self.datenaissance_picker.value.strftime("%d/%m/%Y")
            self.page.update()
    
    def on_date_debut_change(self, e):
        if self.date_debut_picker.value:
            self.formation_date_debut.value = self.date_debut_picker.value.strftime("%d/%m/%Y")
            self.page.update()
    
    def on_date_fin_change(self, e):
        if self.date_fin_picker.value:
            self.formation_date_fin.value = self.date_fin_picker.value.strftime("%d/%m/%Y")
            self.page.update()
    def on_essai_debut_change(self, e):
        if self.essai_debut_picker.value:
            self.formation_essai_debut.value = self.essai_debut_picker.value.strftime("%d/%m/%Y")
            self.page.update()

    def on_essai_fin_change(self, e):
        if self.essai_fin_picker.value:
            self.formation_essai_fin.value = self.essai_fin_picker.value.strftime("%d/%m/%Y")
            self.page.update()

    def on_commune_change(self, commune_dropdown, code_postal_field):
        commune_id = commune_dropdown.value
        if not commune_id:
            code_postal_field.value = ""
            self.page.update()
            return
        conn = DatabaseConfig.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT code_commune FROM COMMUNES WHERE ID = ?", (commune_id,))
            row = cursor.fetchone()
            code_postal_field.value = (row[0] or "").strip() if row else ""
        except Exception as e:
            print(f"Erreur code postal: {e}")
        finally:
            conn.close()
        self.page.update()

    def make_wilaya_commune_widget(self, communes_data):
        conn = DatabaseConfig.get_connection()
        wilayas_data = []
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT ID, wilaya, CODE_WIL FROM WILAYA ORDER BY wilaya")
                wilayas_data = cursor.fetchall()
            except: pass
            finally: conn.close()

        state = {"wilaya_code": None, "commune_id": None, "code_postal": ""}

        wilaya_search = ft.TextField(
            hint_text="Rechercher une wilaya...", border=ft.InputBorder.OUTLINE,
            border_radius=10, height=42, text_size=13,
            prefix_icon=ft.icons.SEARCH, width=400,
        )
        wilaya_label  = ft.Text("—", size=12, color="#6B7280", italic=True)
        wilaya_list   = ft.Column(spacing=2, scroll=ft.ScrollMode.AUTO,
                                  height=120, visible=False)

        commune_search = ft.TextField(
            hint_text="Rechercher une commune...", border=ft.InputBorder.OUTLINE,
            border_radius=10, height=42, text_size=13,
            prefix_icon=ft.icons.SEARCH, width=400, visible=False,
        )
        commune_label  = ft.Text("—", size=12, color="#6B7280", italic=True)
        commune_list   = ft.Column(spacing=2, scroll=ft.ScrollMode.AUTO,
                                   height=120, visible=False)
        code_postal    = ft.TextField(label="Code postal", width=150, read_only=True)

        def item(label, selected, on_click):
            return ft.Container(
                content=ft.Text(label, size=12,
                                color="#FFFFFF" if selected else "#1F2937",
                                overflow=ft.TextOverflow.ELLIPSIS, max_lines=1),
                bgcolor="#20398d" if selected else "#F9FAFB",
                border_radius=8,
                padding=ft.Padding(10, 6, 10, 6),
                border=ft.border.all(1, "#20398d" if selected else "#E5E7EB"),
                on_click=on_click,
            )

        def refresh_commune_list(q=""):
            commune_list.controls = [
                item(
                    c[1],
                    state["commune_id"] == str(c[0]),
                    lambda e, cid=str(c[0]), lbl=c[1], cp=(c[2] or "").strip():
                        select_commune(cid, lbl, cp)
                )
                for c in communes_data
                if c[2] and c[2][:2] == state["wilaya_code"]
                and c[1].lower().startswith(q.lower())
            ]

        def refresh_wilaya_list(q=""):
            wilaya_list.controls = [
                item(
                    w[1],
                    state["wilaya_code"] == w[2],
                    lambda e, wcode=w[2], wlbl=w[1]:
                        select_wilaya(wcode, wlbl)
                )
                for w in wilayas_data
                if (w[1] or "").lower().startswith(q.lower())
            ]

        def select_wilaya(wcode, wlbl):
            state["wilaya_code"] = wcode
            state["commune_id"]  = None
            state["code_postal"] = ""
            wilaya_label.value   = f"✅ {wlbl}"
            wilaya_label.color   = "#20398d"
            wilaya_list.visible  = False
            commune_search.visible = True
            commune_list.visible   = True
            commune_search.value   = ""
            commune_label.value    = "—"
            commune_label.color    = "#6B7280"
            code_postal.value      = ""
            refresh_wilaya_list(wilaya_search.value or "")
            refresh_commune_list()
            self.page.update()

        def select_commune(cid, lbl, cp):
            state["commune_id"]  = cid
            state["code_postal"] = cp
            commune_label.value  = f"✅ {lbl}"
            commune_label.color  = "#20398d"
            code_postal.value    = cp
            commune_list.visible = False
            refresh_commune_list(commune_search.value or "")
            commune_label.update()
            code_postal.update()
            commune_list.update()

        wilaya_search.on_change = lambda e: (
            setattr(wilaya_list, 'visible', bool(e.control.value)),
            refresh_wilaya_list(e.control.value),
            wilaya_list.update()
        )
        commune_search.on_change = lambda e: (
            refresh_commune_list(e.control.value),
            commune_list.update()
        )

        refresh_wilaya_list()

        widget = ft.Column([
            ft.Text("Wilaya", size=12, weight="bold", color="#6B7280"),
            wilaya_search, wilaya_label, wilaya_list,
            ft.Text("Commune", size=12, weight="bold", color="#6B7280"),
            commune_search, commune_label, commune_list,
            code_postal,
        ], spacing=6)

        return widget, state, code_postal
    def load_apprenti_data(self):
        apprenti = get_apprenti_complet(self.apprenti_id)
        if not apprenti:
            return

        self.apprenti_nom.value = apprenti["nom"]
        self.apprenti_prenom.value = apprenti["prenom"]
        self.apprenti_sexe.value = str(apprenti["id_sf"])        
        if apprenti.get("date_naissance"):
            self.apprenti_date_naissance.value = apprenti["date_naissance"].strftime("%d/%m/%Y")        
        self.apprenti_lieu_naissance.value = apprenti.get("lieu_naissance", "")
        self.apprenti_adresse.value = apprenti.get("adresse", "")
        self.apprenti_telephone.value = apprenti.get("telephone", "")
        self.apprenti_email.value = apprenti.get("email", "")

        if apprenti.get("id_com"):
            conn = DatabaseConfig.get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT code_commune, LIB_COMMUNE FROM COMMUNES WHERE ID = ?",
                        (str(apprenti["id_com"]),)
                    )
                    row = cursor.fetchone()
                    if row:
                        code = (row[0] or "")
                        wilaya_code = code[:2] if len(code) >= 2 else None
                        self.app_wc["wilaya_code"] = wilaya_code
                        self.app_wc["commune_id"]  = str(apprenti["id_com"])
                        self.app_wc["code_postal"] = code
                        self.apprenti_code_postal.value = code
                finally:
                    conn.close()

        if apprenti.get("id_niv"):
            self.apprenti_niveau_scolaire.value = str(apprenti["id_niv"])

        if apprenti.get("employeur"):
            emp = apprenti["employeur"]
            self.employeur_denomination.value = emp.get("denomination", "")
            self.employeur_statut_juridique.value = emp.get("statut", "")
            self.employeur_adresse.value = emp.get("adresse", "")
            self.employeur_telephone.value = emp.get("telephone", "")
            self.employeur_fax.value = emp.get("fax", "")
            self.employeur_email.value = emp.get("email", "")
            self.employeur_id_db = apprenti.get("employeur_id")

        self.formation_numero_inscription.value = apprenti.get("ninscript", "")
        self.formation_duree.value = str(apprenti.get("duree", 12))
        if apprenti.get("date_d"):
            self.formation_date_debut.value = apprenti["date_d"].strftime("%d/%m/%Y")        
        if apprenti.get("date_f"):
            self.formation_date_fin.value = apprenti["date_f"].strftime("%d/%m/%Y")        
        self.formation_periode_essai.value = str(apprenti.get("periode_essai", 30))
        if apprenti.get("essai_d"):
            self.formation_essai_debut.value = apprenti["essai_d"].strftime("%d/%m/%Y")       
        if apprenti.get("essai_f"):
            self.formation_essai_fin.value = apprenti["essai_f"].strftime("%d/%m/%Y")
        if apprenti.get("id_sp"):
            self.formation_specialite.value = str(apprenti["id_sp"])
        if apprenti.get("date_resiliation"):
            self.date_resiliation.value = apprenti["date_resiliation"].strftime("%d/%m/%Y")

        self.motif_resiliation.value = apprenti.get("motif_resiliation", "")
        if apprenti.get("photo_path") and apprenti["photo_path"] != "assets/default_avatar.png":
            real_src = PhotoManager.get_photo_display_path(apprenti["photo_path"])
            self.photo_image.content = ft.Image(
                src=real_src,
                width=150, height=150,
                fit=ft.ImageFit.COVER,
                border_radius=ft.border_radius.all(8),
            )
            self.photo_image.bgcolor = None            
            self.photo_status.value = "✅ Photo existante"
            self.photo_status.color = "green"
        else:
            self.photo_image.content = ft.Column(
                [
                    ft.Icon(ft.icons.PHOTO_CAMERA, size=50, color="#9CA3AF"),
                    ft.Text("Aucune photo", size=10, color="#6B7280", text_align=ft.TextAlign.CENTER),
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            )
            self.photo_image.bgcolor = "#E5E7EB"
            self.photo_status.value = "📷 En attente de photo"
            self.photo_status.color = "blue"

        self._original = apprenti

        self.page.update()
    
    def validate_form(self) -> Tuple[bool, str]:
        
        if not self.apprenti_nom.value or not self.apprenti_nom.value.strip():
            return False, "❌ Le nom de l'apprenti est obligatoire"
        if not self.apprenti_prenom.value or not self.apprenti_prenom.value.strip():
            return False, "❌ Le prénom de l'apprenti est obligatoire"
        if not self.apprenti_date_naissance.value:
            return False, "❌ La date de naissance est obligatoire"
        if not self.apprenti_email.value or not self.apprenti_email.value.strip():
            return False, "❌ L'email de l'apprenti est obligatoire"
        
        if "@" not in self.apprenti_email.value:
            return False, "❌ Email invalide"
        
        
        if not self.formation_specialite.value:
            return False, "❌ La spécialité est obligatoire"
        if not self.formation_date_debut.value:
            return False, "❌ La date de début de formation est obligatoire"
        if not self.formation_date_fin.value:
            return False, "❌ La date de fin de formation est obligatoire"
        
        if not self.app_wc["commune_id"]:
            return False, "❌ La commune de l'apprenti est obligatoire"
        
        return True, "✅"
    
    def save(self, e):
            conn = None 
            try:
                is_valid, message = self.validate_form()
                if not is_valid:
                    self.show_snackbar(message)
                    return
                
                conn = DatabaseConfig.get_connection()
                if not conn:
                    self.show_snackbar("❌ Erreur connexion BD")
                    return
                
                cursor = conn.cursor()

                if self.edit_mode:
                    if not self._original:
                        self.show_snackbar("Données originales manquantes")
                        return

                    updates = []
                    values  = []

                    def diff(field_value, original_value, column):
                        v = (field_value or "").strip()
                        o = (original_value or "").strip()
                        if v != o:
                            updates.append(f"{column}=?")
                            values.append(v if v else None)

                    diff(self.apprenti_nom.value,            self._original.get("nom"),            "NOM")
                    diff(self.apprenti_prenom.value,         self._original.get("prenom"),         "PRENOM")
                    diff(self.apprenti_telephone.value,      self._original.get("telephone"),      "TELEPHONE")
                    diff(self.apprenti_email.value,          self._original.get("email"),          "MAIL")
                    diff(self.apprenti_adresse.value,        self._original.get("adresse"),        "ADRESSE")
                    diff(self.apprenti_lieu_naissance.value, self._original.get("lieu_naissance"), "LIEUXNAISSANCE")

                    orig_dn = self._original.get("date_naissance")
                    orig_dn_str = orig_dn.strftime("%d/%m/%Y") if orig_dn else ""
                    if self.apprenti_date_naissance.value != orig_dn_str:
                        updates.append("DATENAISSANCE=?")
                        values.append(
                            datetime.strptime(self.apprenti_date_naissance.value, "%d/%m/%Y").date()
                            if self.apprenti_date_naissance.value else None
                        )
                    if self.edit_mode and self.data.get("id_groupage"):
                        self.apprenti_groupage.value = str(self.data["id_groupage"])

                    orig_dd = self._original.get("date_d")
                    orig_dd_str = orig_dd.strftime("%d/%m/%Y") if orig_dd else ""
                    if self.formation_date_debut.value != orig_dd_str:
                        updates.append("DATE_D=?")
                        values.append(
                            datetime.strptime(self.formation_date_debut.value, "%d/%m/%Y").date()
                            if self.formation_date_debut.value else None
                        )

                    orig_df = self._original.get("date_f")
                    orig_df_str = orig_df.strftime("%d/%m/%Y") if orig_df else ""
                    if self.formation_date_fin.value != orig_df_str:
                        updates.append("DATE_F=?")
                        values.append(
                            datetime.strptime(self.formation_date_fin.value, "%d/%m/%Y").date()
                            if self.formation_date_fin.value else None
                        )

                    new_sp_id = int(self.formation_specialite.value) if self.formation_specialite.value else None
                    if new_sp_id != self._original.get("id_sp"):
                        updates.append("ID_SP=?")
                        values.append(new_sp_id)

                    new_duree = int(self.formation_duree.value or 12)  
                    if new_duree != (self._original.get("duree") or 12):
                        updates.append("DUREE=?")
                        values.append(new_duree)

                    new_essai = int(self.formation_periode_essai.value or 30)  
                    if new_essai != (self._original.get("periode_essai") or 30):
                        updates.append("PERIODE_ESSAI=?")
                        values.append(new_essai)
                    orig_ed = self._original.get("essai_d")
                    orig_ed_str = orig_ed.strftime("%d/%m/%Y") if orig_ed else ""
                    if self.formation_essai_debut.value != orig_ed_str:
                        updates.append("ESSAI_D=?")
                        values.append(datetime.strptime(self.formation_essai_debut.value, "%d/%m/%Y").date()
                                    if self.formation_essai_debut.value else None)
                        
                    orig_ef = self._original.get("essai_f")
                    orig_ef_str = orig_ef.strftime("%d/%m/%Y") if orig_ef else ""
                    if self.formation_essai_fin.value != orig_ef_str:
                        updates.append("ESSAI_F=?")
                        values.append(datetime.strptime(self.formation_essai_fin.value, "%d/%m/%Y").date()
                                    if self.formation_essai_fin.value else None)
                        
                    new_commune_id  = int(self.app_wc["commune_id"]) if self.app_wc["commune_id"] else None
                    orig_commune_id = self._original.get("id_com")
                    if new_commune_id and new_commune_id != orig_commune_id:
                        new_wilaya_id = get_wilaya_by_commune_id(new_commune_id)
                        updates.append("ID_COM=?")
                        values.append(new_commune_id)
                        updates.append("ID_WIL=?")
                        values.append(new_wilaya_id)

                    if self.photo_file_path:
                        success, photo_path = PhotoManager.save_photo(
                            self.apprenti_id, self.photo_file_path
                        )
                        if success:
                            updates.append("photo_path=?")
                            values.append(photo_path)

                    if self._original.get("employeur_id") and self._original.get("employeur"):
                        emp         = self._original["employeur"]
                        emp_updates = []
                        emp_values  = []

                        def diff_emp(field_value, original_value, column):
                            v = (field_value or "").strip()
                            o = (original_value or "").strip()
                            if v != o:
                                emp_updates.append(f"{column}=?")
                                emp_values.append(v if v else None)

                        diff_emp(self.employeur_denomination.value,     emp.get("denomination"), "DENOMINATION")
                        diff_emp(self.employeur_statut_juridique.value, emp.get("statut"),       "STATUT_JURIDIQUE")
                        diff_emp(self.employeur_adresse.value,          emp.get("adresse"),      "ADRESS")
                        diff_emp(self.employeur_telephone.value,        emp.get("telephone"),    "TELEPHONE")
                        diff_emp(self.employeur_fax.value,              emp.get("fax"),          "FAX")
                        diff_emp(self.employeur_email.value,            emp.get("email"),        "EMAIL")

                        if emp_updates:
                            emp_values.append(self._original["employeur_id"])
                            cursor.execute(
                                f"UPDATE EMPLOYEUR SET {', '.join(emp_updates)} WHERE ID=?",
                                emp_values
                            )
                    if self.date_resiliation.value:
                        updates.append("DATE_RESILIATION=?")
                        values.append(
                            datetime.strptime(self.date_resiliation.value, "%d/%m/%Y").date()
                        )

                    if self.motif_resiliation.value:
                        updates.append("MOTIF_RESILIATION=?")
                        values.append(self.motif_resiliation.value.strip())
                    new_groupage = int(self.apprenti_groupage .value) if self.apprenti_groupage .value else None
                    if new_groupage != self._original.get("id_groupage"):
                        updates.append("id_groupage=?")
                        values.append(new_groupage)

                    if updates:
                        values.append(self.apprenti_id)
                        sql = f"UPDATE APPRENTIE SET {', '.join(updates)} WHERE ID=?"
                        cursor.execute(sql, values)
                    
                else:
                    if not self.employeur_id_db:
                        nat_emply_id    = get_nat_employeur_id(self.employeur_type.value)
                        empl_commune_id = int(self.emp_wc["commune_id"])  if self.emp_wc["commune_id"]  else 1
                        empl_wilaya_id  = get_wilaya_by_commune_id(empl_commune_id) or 1

                        cursor.execute("""
                            INSERT INTO EMPLOYEUR (ID_NAT_EMPLY, DENOMINATION, STATUT_JURIDIQUE, ADRESS, ID_WIL, ID_COM, TELEPHONE, FAX, EMAIL)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            nat_emply_id,
                            self.employeur_denomination.value.strip(),
                            self.employeur_statut_juridique.value.strip() if self.employeur_statut_juridique.value else None,
                            self.employeur_adresse.value.strip()          if self.employeur_adresse.value          else None,
                            empl_wilaya_id,
                            empl_commune_id,
                            self.employeur_telephone.value.strip() if self.employeur_telephone.value else None,
                            self.employeur_fax.value.strip()       if self.employeur_fax.value       else None,
                            self.employeur_email.value.strip()     if self.employeur_email.value     else None,
                        ))
                        cursor.execute("SELECT SCOPE_IDENTITY()")
                        self.employeur_id_db = cursor.fetchone()[0]
                
                    commune_id      = int(self.app_wc["commune_id"])  if self.app_wc["commune_id"]  else 1
                    wilaya_id       = get_wilaya_by_commune_id(commune_id)      or 1
                    tuteur_commune_id = int(self.tut_wc["commune_id"]) if self.tut_wc["commune_id"] else None
                    tuteur_wilaya_id  = get_wilaya_by_commune_id(tuteur_commune_id) if tuteur_commune_id else None
                    sp_id = int(self.formation_specialite.value) if self.formation_specialite.value and self.formation_specialite.value.isdigit() else None                    
                    niv_id = int(self.apprenti_niveau_scolaire.value) if self.apprenti_niveau_scolaire.value else None                    
                    sexe_id = get_sexe_id(self.apprenti_sexe.value) or 1                  
                    date_n = datetime.strptime(self.apprenti_date_naissance.value, "%d/%m/%Y").date() if self.apprenti_date_naissance.value else None
                    date_d = datetime.strptime(self.formation_date_debut.value, "%d/%m/%Y").date() if self.formation_date_debut.value else None
                    date_f = datetime.strptime(self.formation_date_fin.value, "%d/%m/%Y").date() if self.formation_date_fin.value else None

                    cursor.execute("""
                        INSERT INTO APPRENTIE (
                            NOM, PRENOM, ID_SF, TELEPHONE, MAIL, DATENAISSANCE, 
                            LIEUXNAISSANCE, ADRESSE, ID_CF, ID_WIL, ID_COM, 
                            DATE_D, DATE_F, ID_SP, ID_SSP, N_CONTRAT, id_groupage, 
                            ID_DIPL, DUREE, ID_NIV, TYP_CONT, ANCIEN_EMPLOYEUR, 
                            AVENANT, DATE_AVENANT, DATED_AVENANT, DATEF_AVENANT, 
                            PERIODE_ESSAI, ESSAI_D, ESSAI_F, NINSCRIPT, 
                            id_employeur, ID_STAT_CONT, DATE_RESILIATION, 
                            MOTIF_RESILIATION, ID_CATEGORIE, photo_path
                        )
                        OUTPUT INSERTED.ID
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        self.apprenti_nom.value.strip(),
                        self.apprenti_prenom.value.strip(),
                        sexe_id or 1,
                        self.apprenti_telephone.value.strip() if self.apprenti_telephone.value else None,
                        self.apprenti_email.value.strip(),
                        date_n,
                        self.apprenti_lieu_naissance.value.strip() if self.apprenti_lieu_naissance.value else None,
                        self.apprenti_adresse.value.strip() if self.apprenti_adresse.value else None,
                        1,
                        wilaya_id or 1,   
                        commune_id or 1,
                        date_d,
                        date_f,
                        sp_id or 54,
                        None,
                        None,
                        int(self.apprenti_groupage.value) if self.apprenti_groupage.value else None,
                        None,
                        int(self.formation_duree.value) if self.formation_duree.value and str(self.formation_duree.value).strip().isdigit() else 12,
                        niv_id or 3,
                        None,
                        None,
                        None,
                        None, None, None,
                        int(self.formation_periode_essai.value) if self.formation_periode_essai.value and str(self.formation_periode_essai.value).strip().isdigit() else 30,
                        None, None,
                        self.formation_numero_inscription.value.strip() if self.formation_numero_inscription.value else None,
                        self.employeur_id_db or 1,
                        None,
                        None if not self.date_resiliation.value else datetime.strptime(self.date_resiliation.value, "%d/%m/%Y").date(),
                        self.motif_resiliation.value.strip() if self.motif_resiliation.value else None,
                        None,
                        None
                    ))
                    row = cursor.fetchone()
                    if not row or row[0] is None:
                        raise Exception("INSERT APPRENTIE a échoué : aucun ID retourné")
                    new_id = int(row[0])
                    print(f"[DEBUG] Nouvel apprenti ID = {new_id}")
                    annee = datetime.now().year
                    cursor.execute("""
                        SELECT MAX(CAST(RIGHT(code_app, 4) AS INT))
                        FROM APPRENTIE 
                        WHERE code_app LIKE ?
                    """, (f"APP-{annee}-%",))
                    row2 = cursor.fetchone()
                    next_num = (row2[0] or 0) + 1
                    code_app = f"APP-{annee}-{next_num:04d}"
                    cursor.execute("UPDATE APPRENTIE SET code_app = ? WHERE ID = ?", (code_app, new_id))
                    print(f"[DEBUG] code_app généré = {code_app}")
                    if self.photo_file_path:
                        success, photo_path_result = PhotoManager.save_photo(new_id, self.photo_file_path)
                        if success:
                            cursor.execute("UPDATE APPRENTIE SET photo_path = ? WHERE ID = ?", (photo_path_result, new_id))
                print(f"[DEBUG] Avant commit")

                conn.commit()
                print(f"[DEBUG] Commit effectué ✅")

                self.show_snackbar("✅ Enregistrement réussi !")
                
                if self.on_saved:
                    self.on_saved()
                self.close(None)

            except Exception as ex:
                if conn: conn.rollback()
                self.show_snackbar(f"❌ Erreur: {str(ex)[:100]}")
                print(f"DEBUG SAVE ERROR: {ex}")
            finally:
                if conn: conn.close()
                    
    def show_snackbar(self, message: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(message))
        self.page.snack_bar.open = True
        self.page.update()
    
    def close(self, e):
        self.root.controls.clear()
        if self.on_close:
            self.on_close()
    
    def show(self):
        self.page.clean() 
        self.root.controls.clear() 
        
        btn_ocr = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.icons.DOCUMENT_SCANNER, size=18),
                ft.Text("Remplir depuis PDF/Image (OCR)", size=14, weight="w600")
            ], spacing=8),
            bgcolor="#10B981",
            color="white",
            height=45,
            on_click=lambda e: self.ocr_file_picker.pick_files(
                allowed_extensions=["pdf", "jpg", "jpeg", "png"],
                dialog_title="Sélectionner un contrat à analyser"
            )
        )
        
        form_controls = [
            ft.Container(
                content=ft.Text("CONTRAT D'APPRENTISSAGE", size=18, weight="bold", color="#20398d", text_align=ft.TextAlign.CENTER),
                alignment=ft.alignment.center,
                padding=20,
            ),
            ft.Divider(height=2, color="#20398d"),
            
            ft.Container(
                content=btn_ocr,
                alignment=ft.alignment.center,
                padding=ft.padding.only(top=10, bottom=15)
            ),
            ft.Divider(),
            
            ft.Container(ft.Text("1️⃣ EMPLOYEUR", size=14, weight="bold", color="white"), bgcolor="#20398d", padding=8),
            ft.Text("Type d'employeur:", size=12, weight="bold"),
            self.employeur_type,
            ft.Column([self.employeur_denomination], spacing=10),
            ft.Column([self.employeur_statut_juridique], spacing=10),
            ft.Column([self.employeur_adresse], spacing=10),
            self.emp_wc_widget,
            ft.Row([self.employeur_telephone, self.employeur_fax], spacing=10),
            ft.Column([self.employeur_email], spacing=10),
            ft.Divider(),
            
            ft.Container(ft.Text("2️⃣ APPRENTI(E) + PHOTO", size=14, weight="bold", color="white"), bgcolor="#20398d", padding=8),
            ft.Row([
                ft.Column([
                    ft.Row([self.apprenti_nom, self.apprenti_prenom], spacing=10),
                    ft.Container(
                        content=self.apprenti_date_naissance,
                        on_click=lambda e: self.datenaissance_picker.pick_date(),
                    ),
                    ft.Column([self.apprenti_lieu_naissance], spacing=10),
                    ft.Text("Sexe:", size=12, weight="bold"),
                    self.apprenti_sexe,
                    ft.Row([self.apprenti_groupage], spacing=10),
                    ft.Column([self.apprenti_adresse], spacing=10),
                    self.app_wc_widget,
                    ft.Row([self.apprenti_telephone, self.apprenti_email], spacing=10),
                    ft.Column([self.apprenti_niveau_scolaire], spacing=10),
                ], expand=True),
                
                ft.Container(
                    width=180, 
                    padding=ft.padding.only(left=20), 
                    content=ft.Column(
                        controls=[
                            ft.Text("PHOTO", size=12, weight="bold", color=ft.colors.BLUE_GREY_700),
                            self.photo_image, 
                            self.btn_select_photo,
                            self.photo_status,
                        ],
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        spacing=10,
                    ),
                )
            ], spacing=20, vertical_alignment=ft.CrossAxisAlignment.START),
            ft.Divider(),
            
            ft.Container(ft.Text("TUTEUR", size=14, weight=ft.FontWeight.BOLD, color="white"), bgcolor="#20398d", padding=8),
            ft.Row([self.tuteur_nom, self.tuteur_prenom], spacing=10),
            ft.Column([self.tuteur_adresse], spacing=10),
            self.tut_wc_widget,
            ft.Row([self.tuteur_telephone, self.tuteur_email], spacing=10),
            ft.Divider(),
            
            ft.Container(ft.Text("3️⃣ ÉTABLISSEMENT FORMATION", size=14, weight="bold", color="white"), bgcolor="#20398d", padding=8),
            ft.Column([self.formation_denomination], spacing=10),
            ft.Column([self.formation_adresse], spacing=10),
            self.form_wc_widget,
            ft.Row([self.formation_telephone, self.formation_fax, self.formation_email], spacing=10),
            ft.Divider(),
            
            ft.Container(ft.Text("4️⃣ FORMATION", size=14, weight="bold", color="white"), bgcolor="#20398d", padding=8),
            ft.Row([self.formation_code, self.formation_specialite], spacing=10),
            ft.Column([self.formation_numero_inscription], spacing=10),
            ft.Column([self.formation_duree], spacing=10),
            ft.Row([
                ft.Container(content=self.formation_date_debut, on_click=lambda e: self.date_debut_picker.pick_date()),
                ft.Container(content=self.formation_date_fin, on_click=lambda e: self.date_fin_picker.pick_date()),
            ], spacing=10),
            ft.Column([self.formation_diplome], spacing=10),
            ft.Row([
                self.formation_periode_essai,
                ft.Container(content=self.formation_essai_debut,
                            on_click=lambda e: self.essai_debut_picker.pick_date()),
                ft.Container(content=self.formation_essai_fin,
                            on_click=lambda e: self.essai_fin_picker.pick_date()),
            ], spacing=10),            
            ft.Container(
                visible=self.edit_mode, 
                content=ft.Column([
                    ft.Container(ft.Text("5️⃣ RÉSILIATION", size=14, weight="bold", color="white"), bgcolor="#20398d", padding=8),
                    ft.Row([
                        ft.Container(
                            content=self.date_resiliation,
                            on_click=lambda e: self.date_fin_picker.pick_date()
                        ),
                        self.motif_resiliation
                    ], spacing=10),
                    ft.Divider(),
                ], spacing=10),
            ),
            ft.Divider(),
            
            ft.Container(
                content=ft.Row([self.btn_save, self.btn_cancel], spacing=10, alignment=ft.MainAxisAlignment.CENTER),
                padding=20,
            ),
            ft.Container(height=20),
        ]
        
        self.root.controls.extend(form_controls)
        self.page.add(
            ft.Container(
                content=self.root,
                expand=True,
                padding=25,
                alignment=ft.alignment.top_center,
            )
        )
        self.page.update()
        if self.edit_mode and self.apprenti_id:
            self.load_apprenti_data()
    def on_ocr_file_result(self, e):
        if not e.files:
            return
        
        file_path = e.files[0].path
        
        self.page.snack_bar = ft.SnackBar(
            ft.Text("⏳ Extraction des données en cours..."),
            duration=3000
        )
        self.page.snack_bar.open = True
        self.page.update()
        
        try:
            from OCR import PrecisionExtractor
            
            json_path = "Boxes.json"
            
            if not os.path.exists(json_path):
                self.show_snackbar("❌ Fichier Boxes.json introuvable")
                return
            
            extractor = PrecisionExtractor(json_path)
            img = extractor.get_image_from_file(file_path)
            
            if img is None:
                self.show_snackbar("❌ Impossible de lire l'image/PDF")
                return
            
            resultats = extractor.extract_with_logic(img)
            
            self.remplir_depuis_ocr(resultats)
            
            self.show_snackbar("✅ Données extraites avec succès !")
            
        except ImportError:
            self.show_snackbar("❌ Module OCR non installé. Installez: pip install opencv-python pytesseract pdf2image")
        except Exception as ex:
            self.show_snackbar(f"❌ Erreur OCR : {str(ex)[:100]}")
            print(f"Détails erreur OCR : {ex}")

    def remplir_depuis_ocr(self, resultats):
        
        mapping = {
            'app_nom': self.apprenti_nom,
            'app_prenom': self.apprenti_prenom,
            'app_date_naiss': self.apprenti_date_naissance,
            'app_lieu_naiss': self.apprenti_lieu_naissance,
            'app_tel': self.apprenti_telephone,
            'app_email': self.apprenti_email,
            'app_adresse': self.apprenti_adresse,
            
            'etab_nom': self.formation_denomination,
            'etab_adresse': self.formation_adresse,
            'etab_tel': self.formation_telephone,
            'etab_email': self.formation_email,
            
            'f_code': self.formation_code,
            'f_date_debut': self.formation_date_debut,
            'f_date_fin': self.formation_date_fin,
            'f_duree': self.formation_duree,
            'f_essai': self.formation_periode_essai,
        }
        
        for cle_json, champ_flet in mapping.items():
            if cle_json in resultats and resultats[cle_json]:
                valeur = resultats[cle_json]
                if valeur and len(str(valeur).strip()) > 0:
                    champ_flet.value = str(valeur).strip()
        
        self.page.update()
def get_apprentis() -> list:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        query = """
        SELECT 
            A.ID,
            A.NOM,
            A.PRENOM,
            A.MAIL,
            A.TELEPHONE,
            A.photo_path,
            A.DATENAISSANCE,
            A.DATE_D,
            A.DATE_F,
            A.ID_SP,
            A.DUREE,
            A.PERIODE_ESSAI,
            A.LIEUXNAISSANCE,
            A.ADRESSE,
            A.ID_WIL,
            A.ID_COM,
            A.ID_SF,
            A.ESSAI_D,
            A.ESSAI_F,
            A.NINSCRIPT,
            E.DENOMINATION      AS SOCIETE,
            E.STATUT_JURIDIQUE,
            E.TELEPHONE         AS TEL_EMPLOYEUR,
            E.EMAIL             AS EMAIL_EMPLOYEUR,
            S.LIB_SSP           AS SOUS_SPECIALITE,
            B.LIBELLE_SP        AS BRANCHE,
            N.num_niveau        AS NIVEAU,
            SC.STATUT_CONTRAT   AS STATUT_CONTRAT,
            A.code_app
        FROM APPRENTIE A
        LEFT JOIN EMPLOYEUR       E  ON A.id_employeur  = E.ID
        LEFT JOIN SPECIALITE      S  ON A.ID_SP         = S.ID
        LEFT JOIN BRANCHE         B  ON S.ID_SP         = B.ID
        LEFT JOIN NIVEAU          N  ON A.ID_NIV        = N.ID
        LEFT JOIN STATUT_CONTRAT  SC ON A.ID_STAT_CONT  = SC.ID
        ORDER BY A.NOM, A.PRENOM
        """
        cursor.execute(query)
        apprentis = cursor.fetchall()

        result = []
        for row in apprentis:
            photo_path = (
                PhotoManager.get_photo_display_path(row[5])
                if row[5] else "assets/default_avatar.png"
            )
            sexe = "Masculin" if row[16] == 1 else "Féminin" if row[16] == 2 else ""

            def fmt(d):
                return d.strftime("%d/%m/%Y") if d else ""

            result.append({
                'ID':               row[0],
                'NOM':              row[1]  or '',
                'PRENOM':           row[2]  or '',
                'MAIL':             row[3]  or '',
                'TELEPHONE':        row[4]  or '',
                'photo_path':       photo_path,
                'DATENAISSANCE':    fmt(row[6]),
                'DATE_D':           fmt(row[7]),
                'DATE_F':           fmt(row[8]),
                'ID_SP':            row[9],
                'DUREE':            row[10] or 0,
                'PERIODE_ESSAI':    row[11] or 0,
                'LIEU_NAISSANCE':   row[12] or '',
                'ADRESSE':          row[13] or '',
                'ID_WIL':           row[14],
                'ID_COM':           row[15],
                'SEXE':             sexe,
                'ID_SF':            row[16],
                'ESSAI_D':          fmt(row[17]),
                'ESSAI_F':          fmt(row[18]),
                'NINSCRIPT':        row[19] or '',
                'SOCIETE':          row[20] or '',
                'STATUT_JURIDIQUE': row[21] or '',
                'TEL_EMPLOYEUR':    row[22] or '',
                'EMAIL_EMPLOYEUR':  row[23] or '',
                'SOUS_SPECIALITE':  row[24].strip() if row[24] else '',
                'BRANCHE':          row[25].strip() if row[25] else '',
                'NIVEAU':           row[26] or '',
                'STATUT_CONTRAT':   (row[27] or '').strip(),
                'code_app':         row[28] or '',
            })
        return result

    except Exception as e:
        print(f"[ERREUR] get_apprentis: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_apprenti_complet(apprenti_id: int) -> dict:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                A.ID,                           -- 0
                A.NOM,                          -- 1
                A.PRENOM,                       -- 2
                A.ID_SF,                        -- 3
                A.TELEPHONE,                    -- 4
                A.MAIL,                         -- 5
                A.DATENAISSANCE,                -- 6
                A.LIEUXNAISSANCE,               -- 7
                A.ADRESSE,                      -- 8
                A.ID_WIL,                       -- 9
                A.ID_COM,                       -- 10
                A.DATE_D,                       -- 11
                A.DATE_F,                       -- 12
                A.ID_SP,                        -- 13
                A.DUREE,                        -- 14
                A.ID_NIV,                       -- 15
                A.PERIODE_ESSAI,                -- 16
                A.ESSAI_D,                      -- 17
                A.ESSAI_F,                      -- 18
                A.NINSCRIPT,                    -- 19
                A.id_employeur,                 -- 20
                A.photo_path,                   -- 21
                S.LIB_SSP           AS SOUS_SPECIALITE,  -- 22
                B.LIBELLE_SP        AS BRANCHE,          -- 23
                NS.NIVEAU_SCOL      AS NIVEAU_SCOLAIRE,  -- 24
                G.groupage          AS GROUPAGE,         -- 25
                A.code_app,                     -- 26
                A.N_CONTRAT,                    -- 27
                A.ID_DIPL,                      -- 28
                A.ID_CF,                        -- 29
                S.code_sp           AS CODE_SP,          -- 30
                D.LIB_DIPLOME       AS DIPLOME,          -- 31
                W.wilaya            AS WILAYA,           -- 32
                CM.LIB_COMMUNE      AS COMMUNE,          -- 33
                TC.type_centre      AS TYPE_CENTRE,      -- 34
                CF.adresse          AS CF_ADRESSE,       -- 35
                CF.telephone        AS CF_TELEPHONE,     -- 36
                CF.fax              AS CF_FAX,           -- 37
                CF.mail             AS CF_MAIL,          -- 38
                WCF.wilaya          AS CF_WILAYA,        -- 39
                CCF.LIB_COMMUNE     AS CF_COMMUNE,       -- 40
                SC.STATUT_CONTRAT   AS STATUT_CONTRAT,   -- 41
                A.DATE_RESILIATION,                      -- 42
                A.MOTIF_RESILIATION                      -- 43
            FROM APPRENTIE A
            LEFT JOIN SPECIALITE        S   ON A.ID_SP          = S.ID
            LEFT JOIN BRANCHE           B   ON S.ID_SP          = B.ID
            LEFT JOIN NIVEAU_SCOLAIRE   NS  ON A.ID_NIV         = NS.ID
            LEFT JOIN GROUPAGE          G   ON A.id_groupage    = G.ID
            LEFT JOIN DIPLOME           D   ON A.ID_DIPL        = D.ID
            LEFT JOIN WILAYA            W   ON A.ID_WIL         = W.ID
            LEFT JOIN COMMUNES          CM  ON A.ID_COM         = CM.ID
            LEFT JOIN CENTRE_FORMATION  CF  ON A.ID_CF          = CF.ID
            LEFT JOIN TYPE_CENTRE       TC  ON CF.id_typecentre = TC.ID
            LEFT JOIN WILAYA            WCF ON CF.id_w          = WCF.ID
            LEFT JOIN COMMUNES          CCF ON CF.id_com        = CCF.ID
            LEFT JOIN STATUT_CONTRAT    SC  ON A.ID_STAT_CONT   = SC.ID
            WHERE A.ID = ?
        """, (apprenti_id,))

        row = cursor.fetchone()
        if not row:
            return None

        employeur = None
        if row[20]:
            cursor.execute("""
                SELECT ID, DENOMINATION, STATUT_JURIDIQUE, ADRESS,
                       TELEPHONE, FAX, EMAIL, ID_COM, ID_WIL
                FROM EMPLOYEUR
                WHERE ID = ?
            """, (row[20],))
            employeur = cursor.fetchone()

            emp_commune = ""
            emp_wilaya  = ""
            if employeur:
                if employeur[7]:  
                    cursor.execute(
                        "SELECT LIB_COMMUNE FROM COMMUNES WHERE ID = ?",
                        (employeur[7],)
                    )
                    r = cursor.fetchone()
                    emp_commune = (r[0] or "").strip() if r else ""

                if employeur[8]: 
                    cursor.execute(
                        "SELECT wilaya FROM WILAYA WHERE ID = ?",
                        (employeur[8],)
                    )
                    r = cursor.fetchone()
                    emp_wilaya = (r[0] or "").strip() if r else ""

        return {
            "id":               row[0],
            "nom":              row[1],
            "prenom":           row[2],
            "id_sf":            row[3],
            "sexe":             "Masculin" if row[3] == 1 else "Féminin" if row[3] == 2 else "",
            "telephone":        row[4],
            "email":            row[5],
            "date_naissance":   row[6],
            "lieu_naissance":   row[7],
            "adresse":          row[8],
            "id_wil":           row[9],
            "wilaya":           (row[32] or "").strip(),
            "id_com":           row[10],
            "commune":          (row[33] or "").strip(),
            "date_d":           row[11],
            "date_f":           row[12],
            "id_sp":            row[13],
            "duree":            row[14],
            "id_niv":           row[15],
            "periode_essai":    row[16],
            "essai_d":          row[17],
            "essai_f":          row[18],
            "ninscript":        row[19],
            "employeur_id":     row[20],
            "photo_path":       row[21],
            "sous_specialite":  (row[22] or "").strip(),
            "branche":          (row[23] or "").strip(),
            "code_sp":          (row[30] or "").strip(),
            "niveau_scolaire":  (row[24] or "").strip(),
            "groupage":         (row[25] or "").strip(),
            "code_app":         row[26],
            "n_contrat":        row[27],
            "diplome":          (row[31] or "").strip(),
            "id_cf":            row[29],
            "cf_type":          (row[34] or "").strip(),
            "cf_adresse":       (row[35] or "").strip(),
            "cf_telephone":     (row[36] or "").strip(),
            "cf_fax":           (row[37] or "").strip(),
            "cf_mail":          (row[38] or "").strip(),
            "cf_wilaya":          (row[39] or "").strip(),
            "cf_commune":         (row[40] or "").strip(),
            "statut_contrat":     (row[41] or "").strip(),  
            "date_resiliation":   row[42],                   
            "motif_resiliation":  (row[43] or "").strip(),  
            "employeur": {
                "id":           employeur[0],
                "denomination": employeur[1],
                "statut":       employeur[2],
                "adresse":      employeur[3],
                "telephone":    employeur[4],
                "fax":          employeur[5],
                "email":        employeur[6],
                "commune":      emp_commune,
                "wilaya":       emp_wilaya,
            } if employeur else None,
        }

    except Exception as e:
        print(f"[ERREUR] get_apprenti_complet: {e}")
        return None
    finally:
        conn.close()
def delete_apprenti(apprenti_id: int) -> Tuple[bool, str]:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return False, "Erreur connexion BD"
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT photo_path FROM APPRENTIE WHERE ID = ?", (apprenti_id,))
        row = cursor.fetchone()
        
        if row and row[0]:
            PhotoManager.delete_photo(row[0])
        
        cursor.execute("DELETE FROM APPRENTIE WHERE ID = ?", (apprenti_id,))
        
        conn.commit()        
        return True, f"Apprenti supprimé avec succès"
    
    except Exception as e:
        print(f"[ERREUR] delete_apprenti: {e}")
        return False, f"Erreur suppression: {str(e)[:100]}"
    finally:
        if conn:
            conn.close()


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def verify_login(nom: str, password: str) -> Tuple[bool, Dict | str]:
    conn = DatabaseConfig.get_connection()
    if not conn:
        return False, "Erreur de connexion à la base de données"
    
    try:
        cursor = conn.cursor()
        hashed_password = hash_password(password)
        
        query = """
        SELECT ID, NOM, PRENOM, EMAIL, IMAGE, ACTIF
        FROM UTILISATEURS
        WHERE LTRIM(RTRIM(NOM)) = ? AND MOT_DE_PASSE = ?
        """
        
        cursor.execute(query, (nom.strip(), hashed_password))
        user = cursor.fetchone()
        
        if not user:
            return False, "Nom d'utilisateur ou mot de passe incorrect"
        
        if user[5] == 0:
            return False, "Compte utilisateur désactivé"
        
        image_src = "assets/Has.jpeg"
        
        if user[4]:
            try:
                image_base64 = base64.b64encode(user[4]).decode('utf-8')
                image_src = f"data:image/jpeg;base64,{image_base64}"
            except:
                pass
        
        return True, {
            'id': user[0],
            'nom': user[1],
            'prenom': user[2],
            'email': user[3],
            'image': image_src
        }
            
    except Exception as e:
        print(f"[ERREUR] verify_login: {e}")
        return False, f"Erreur: {str(e)}"
    
    finally:
        if conn:
            conn.close()


def login_page(page: ft.Page, on_login_success) -> ft.Row:
    page.title = "Connexion - BTPH"
    page.window_width = 1200
    page.window_height = 700
    page.window_resizable = False
    page.window_icon = "assets/BTPH.ico"
    page.padding = 0
    page.spacing = 0
    
    page.fonts = {
        "Poppins": "https://fonts.gstatic.com/s/poppins/v20/pxiEyp8kv8JHgFVrJJfecg.woff2",
        "PoppinsBold": "https://fonts.gstatic.com/s/poppins/v20/pxiByp8kv8JHgFVrLCz7Z1xlFQ.woff2",
        "Inter": "https://fonts.gstatic.com/s/inter/v12/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuLyfAZ9hiA.woff2"
    }
    
    def clear_errors(e=None):
        username.error_text = ""
        password_field.error_text = ""
        status_text.value = ""
        page.update()
    
    def validate_form() -> bool:
        is_valid = True
        
        if not username.value or not username.value.strip():
            username.error_text = "Le nom d'utilisateur est requis"
            is_valid = False
        
        if not password_field.value:
            password_field.error_text = "Le mot de passe est requis"
            is_valid = False
        
        page.update()
        return is_valid
    
    def login_clicked(e):
        clear_errors()
        
        if not validate_form():
            return
        
        status_text.value = "⏳ Vérification en cours..."
        status_text.color = "#F59E0B"
        page.update()
        
        success, result = verify_login(username.value, password_field.value)
        
        if success:
            status_text.value = f"✅ Bienvenue {result['prenom']} {result['nom']}!"
            status_text.color = "#4ADE80"
            page.update()
            
            username.value = ""
            password_field.value = ""
            on_login_success(result)
        else:
            status_text.value = f"❌ {result}"
            status_text.color = "#EF4444"
            page.update()
    
    username = ft.TextField(
        label="Nom d'utilisateur",
        border_radius=12,
        filled=True,
        bgcolor="#F9FAFB",
        color="#000000",
        label_style=ft.TextStyle(color="#20398d", font_family="Poppins", size=14),
        text_style=ft.TextStyle(font_family="Inter", size=15),
        border_color="#E9D5FF",
        focused_border_color="#20398d",
        on_change=clear_errors,
        height=60,
        prefix_icon=ft.icons.PERSON,
    )
    
    password_field = ft.TextField(
        label="Mot de passe",
        hint_text="••••••••",
        password=True,
        can_reveal_password=True,
        border_radius=12,
        filled=True,
        bgcolor="#F9FAFB",
        color="#000000",
        label_style=ft.TextStyle(color="#20398d", font_family="Poppins", size=14),
        text_style=ft.TextStyle(font_family="Inter", size=15),
        border_color="#E9D5FF",
        focused_border_color="#20398d",
        on_change=clear_errors,
        height=60,
        prefix_icon=ft.icons.LOCK,
        on_submit=login_clicked,
    )
    
    status_text = ft.Text(
        "",
        size=14,
        text_align=ft.TextAlign.CENTER,
        font_family="Poppins"
    )
    
    return ft.Row(
        [
            ft.Container(
                content=ft.Column(
                    [
                        ft.Container(height=60),
                        ft.Image(
                            src="assets/BTPH.jpeg",
                            width=280,
                            height=280,
                        ),
                        ft.Container(height=20),
                        ft.Text(
                            "Digitalisez le suivi de\n vos apprentis",
                            size=32,
                            color="#20398d",
                            text_align=ft.TextAlign.CENTER,
                            font_family="PoppinsBold",
                            weight=ft.FontWeight.BOLD,
                        ),
                        ft.Container(height=10),
                        ft.Row(
                            [
                                ft.Text(
                                    "en toute sécurité",
                                    size=18,
                                    color="#6B7280",
                                    font_family="Inter",
                                    weight=ft.FontWeight.W_500,
                                ),
                                ft.Icon(ft.icons.VERIFIED, color="#20398d", size=22)
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            spacing=8,
                        ),
                    ],
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    alignment=ft.MainAxisAlignment.CENTER,
                ),
                width=600,
                bgcolor="#FFFFFF",
                expand=True,
            ),
            
            ft.Container(
                content=ft.Column(
                    [
                        ft.Container(height=80),
                        ft.Text(
                            "Connexion",
                            size=36,
                            color="#20398d",
                            font_family="PoppinsBold",
                            weight=ft.FontWeight.BOLD,
                        ),
                        ft.Text(
                            "Entrez vos identifiants pour continuer",
                            size=16,
                            color="#6B7280",
                            font_family="Inter"
                        ),
                        ft.Container(height=40),
                        username,
                        ft.Container(height=20),
                        password_field,
                        ft.Container(height=30),
                        ft.FilledButton(
                            content=ft.Row(
                                [
                                    ft.Text(
                                        "SE CONNECTER",
                                        size=16,
                                        weight=ft.FontWeight.BOLD,
                                        font_family="PoppinsBold"
                                    ),
                                    ft.Icon(ft.icons.ARROW_FORWARD, size=20),
                                ],
                                alignment=ft.MainAxisAlignment.CENTER,
                                spacing=10,
                            ),
                            width=280,
                            height=55,
                            on_click=login_clicked,
                            style=ft.ButtonStyle(
                                bgcolor="#20398d",
                                shape=ft.RoundedRectangleBorder(radius=12),
                            )
                        ),
                        ft.Container(height=20),
                        status_text,
                    ],
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    spacing=0,
                ),
                padding=50,
                expand=True,
                bgcolor="#FAFAFA",
            ),
        ],
        spacing=0,
        expand=True,
    )

def gui_page(page: ft.Page, user_info: Dict, on_logout, show_login, show_gui) -> ft.Row:
    page.title = "BTPH - Gestion des Apprentis"
    page.window_maximized = True
    page.bgcolor = "#F5F5F7"
    page.padding = 0
    page.spacing = 0
    
    selected_menu = {
        "type": "all",  
        "id": None
    }    
    view_mode = {"current": "grid"}
    apprentis_data = []
    search_query = {"current": ""}
    show_profile_menu = {"current": False}
    sidebar_open = True
    sidebar = ft.Container(
        width=260,
        bgcolor="#FFFFFF",
        padding=6,
        shadow=ft.BoxShadow(
            spread_radius=0,
            blur_radius=10,
            color="#0000000A",
            offset=ft.Offset(2, 0),
        ),
    )

    specialites     = load_sous_specialites()
    maitres_sidebar = load_maitres_sidebar()
    projets_sidebar = load_projets_sidebar()

    def toggle_sidebar(e):
        nonlocal sidebar_open
        sidebar_open = not sidebar_open
        build_sidebar()
        page.update()
    def sidebar_header():
        return ft.Container(
        padding=12,
        content=ft.Stack(
            [
                ft.Row(
                    [
                        ft.IconButton(
                            icon=ft.icons.BUSINESS,
                            icon_size=26,
                            icon_color="#20398d",  
                            on_click=toggle_sidebar,
                        ),
                        ft.Text(
                            "BTPH",
                            size=18,
                            weight="bold",
                            color="#20398d",
                            visible=sidebar_open,
                        ),
                    ],
                ),
            ],
        ),
    )
    def fixed_menu():
        return ft.Column(
            [
                menu_item(ft.icons.DASHBOARD, "Tous", "all", None),
            ],
            spacing=6,
        )   
    def logout_button():
        return menu_item(
            ft.icons.LOGOUT,
            "Déconnexion",
            "logout",
            None,
        )
    def menu_item(icon, label, menu_type="all", sp_id=None):
        is_selected = (
            selected_menu["type"] == menu_type
            and selected_menu["id"] == sp_id
        )

        def on_click(e, _type=menu_type, _id=sp_id):
            if _type == "logout":
                on_logout()
                return
            selected_menu["type"] = _type
            selected_menu["id"] = _id
            load_apprentis(_id)
            build_sidebar()
            page.update()

        return ft.Container(
            padding=ft.Padding(14, 14, 14, 14),
            bgcolor="#20398d" if is_selected else "transparent",
            border_radius=8,
            content=ft.Row(
                [
                    ft.Icon(
                        icon,
                        size=22,
                        color="#EF4444" if menu_type == "logout"
                        else ("#FFFFFF" if is_selected else "#20398d"),
                    ) if icon else ft.Container(width=4),
                    ft.Text(
                        label,
                        size=16,
                        weight="w600",
                        color="#EF4444" if menu_type == "logout"
                        else ("#FFFFFF" if is_selected else "#1F2937"),
                        visible=sidebar_open,
                        overflow=ft.TextOverflow.ELLIPSIS, 
                        max_lines=1,                        
                        expand=True,                        
                    ),
                ],
                spacing=8,
                alignment=ft.MainAxisAlignment.CENTER if not sidebar_open
                          else ft.MainAxisAlignment.START,
            ),
            on_click=on_click,
        )
    
    sections_open = {"specialites": False, "maitres": False, "projets": False}

    sp_list_col = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO, expand=True, visible=False)
    ma_list_col = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO, expand=True, visible=False)
    pr_list_col = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO, expand=True, visible=False)

    def refresh_sp_list(q=""):
        sp_list_col.controls = [
            menu_item(None, sp[3].strip('"'), "specialite", sp[0])
            for sp in specialites if sp[3].lower().startswith(q.lower())
        ]

    def refresh_ma_list(q=""):
        ma_list_col.controls = [
            menu_item(None, f"{m[1]} {m[2]}", "maitre", m[0])
            for m in maitres_sidebar if f"{m[1]} {m[2]}".lower().startswith(q.lower())
        ]

    def refresh_pr_list(q=""):
        pr_list_col.controls = [
            menu_item(None, p[1], "projet", p[0])
            for p in projets_sidebar if p[1].lower().startswith(q.lower())
        ]

    refresh_sp_list()
    refresh_ma_list()
    refresh_pr_list()

    sp_search_field = ft.TextField(
        hint_text="Rechercher...", border=ft.InputBorder.NONE,
        height=38, text_size=13, content_padding=ft.Padding(8, 0, 8, 0),
        visible=False,
    )
    ma_search_field = ft.TextField(
        hint_text="Rechercher...", border=ft.InputBorder.NONE,
        height=38, text_size=13, content_padding=ft.Padding(8, 0, 8, 0),
        visible=False,
    )
    pr_search_field = ft.TextField(
        hint_text="Rechercher...", border=ft.InputBorder.NONE,
        height=38, text_size=13, content_padding=ft.Padding(8, 0, 8, 0),
        visible=False,
    )


    def on_sp_search(e):
        refresh_sp_list(e.control.value)
        page.update()

    def on_ma_search(e):
        refresh_ma_list(e.control.value)
        page.update()

    def on_pr_search(e):
        refresh_pr_list(e.control.value)
        page.update()

    sp_search_field.on_change = on_sp_search
    ma_search_field.on_change = on_ma_search
    pr_search_field.on_change = on_pr_search

    def make_search_container(field):
        return ft.Container(
            margin=ft.Margin(10, 4, 10, 6),
            bgcolor="#F3F4F6", border_radius=10,
            padding=ft.Padding(8, 2, 8, 2),
            visible=False,
            content=ft.Row([
                ft.Icon(ft.icons.SEARCH, size=16, color="#9CA3AF"),
                field,
            ], spacing=6),
        )

    sp_search_container = make_search_container(sp_search_field)
    ma_search_container = make_search_container(ma_search_field)
    pr_search_container = make_search_container(pr_search_field)

    sp_arrow = ft.Icon(ft.icons.KEYBOARD_ARROW_RIGHT, size=20, color="#20398d")
    ma_arrow = ft.Icon(ft.icons.KEYBOARD_ARROW_RIGHT, size=20, color="#20398d")
    pr_arrow = ft.Icon(ft.icons.KEYBOARD_ARROW_RIGHT, size=20, color="#20398d")

    def toggle_section(key):
        was_open = sections_open[key]
        for k in sections_open:
            sections_open[k] = False
        if not was_open:
            sections_open[key] = True

        sp_arrow.name = ft.icons.KEYBOARD_ARROW_DOWN if sections_open["specialites"] else ft.icons.KEYBOARD_ARROW_RIGHT
        ma_arrow.name = ft.icons.KEYBOARD_ARROW_DOWN if sections_open["maitres"]     else ft.icons.KEYBOARD_ARROW_RIGHT
        pr_arrow.name = ft.icons.KEYBOARD_ARROW_DOWN if sections_open["projets"]     else ft.icons.KEYBOARD_ARROW_RIGHT

        sp_search_container.visible = sections_open["specialites"]
        sp_search_field.visible     = sections_open["specialites"]
        sp_list_col.visible         = sections_open["specialites"]

        ma_search_container.visible = sections_open["maitres"]
        ma_search_field.visible     = sections_open["maitres"]
        ma_list_col.visible         = sections_open["maitres"]

        pr_search_container.visible = sections_open["projets"]
        pr_search_field.visible     = sections_open["projets"]
        pr_list_col.visible         = sections_open["projets"]

        sp_search_field.value = ""
        ma_search_field.value = ""
        pr_search_field.value = ""
        refresh_sp_list()
        refresh_ma_list()
        refresh_pr_list()

        sp_arrow.update()
        ma_arrow.update()
        pr_arrow.update()
        sp_search_container.update()
        ma_search_container.update()
        pr_search_container.update()
        sp_list_col.update()
        ma_list_col.update()
        pr_list_col.update()

    def make_section_header(key, icon, title, arrow):
        return ft.Container(
            padding=ft.Padding(14, 12, 10, 12),
            on_click=lambda e, k=key: toggle_section(k),
            bgcolor="#F8FAFC",
            border_radius=8,
            margin=ft.Margin(4, 2, 4, 2),
            content=ft.Row([
                ft.Icon(icon, size=20, color="#20398d"),
                ft.Text(
                    title, color="#20398d", size=14, weight="bold",
                    overflow=ft.TextOverflow.ELLIPSIS, max_lines=1, expand=True,
                ),
                arrow,
            ], spacing=10),
        )

    sp_header = make_section_header("specialites", ft.icons.CATEGORY,        "Spécialités",             sp_arrow)
    ma_header = make_section_header("maitres",     ft.icons.PERSON,           "Maîtres d'apprentissage", ma_arrow)
    pr_header = make_section_header("projets",     ft.icons.BUSINESS_CENTER,  "Projets",                 pr_arrow)

    def build_sidebar():
        nonlocal sidebar, sidebar_open
        sidebar.width = 260 if sidebar_open else 72
        sidebar.content = ft.Column(
            [
                sidebar_header(),
                ft.Divider(height=1),
                fixed_menu(),
                ft.Divider(height=1),

                ft.Container(
                    expand=True,
                    content=ft.Column([
                        sp_header,
                        sp_search_container,
                        sp_list_col,
                        ft.Divider(height=1),
                        ma_header,
                        ma_search_container,
                        ma_list_col,
                        ft.Divider(height=1),
                        pr_header,
                        pr_search_container,
                        pr_list_col,
                    ], spacing=0, expand=True),
                ),

                ft.Divider(height=1),
                logout_button(),
            ],
            spacing=0,
            expand=True,
        )
    def load_apprentis(filter_id=None):
        nonlocal apprentis_data
        _type = selected_menu["type"]
        
        if _type == "specialite" and filter_id is not None:
            apprentis_data = get_apprentis_by_specialite(filter_id)
        elif _type == "maitre" and filter_id is not None:
            apprentis_data = get_apprentis_by_maitre(filter_id)
        elif _type == "projet" and filter_id is not None:
            apprentis_data = get_apprentis_by_projet(filter_id)
        else:
            apprentis_data = get_apprentis()
        
        apprentis_data = [normalize(a) for a in apprentis_data]
        build_sidebar()
        update_content_area()


    def _lancer_generation_pdf(page, func, *args):
        def run():
            page.snack_bar = ft.SnackBar(
                ft.Text("⏳ Génération du PDF en cours..."),
                duration=3000
            )
            page.snack_bar.open = True
            page.update()
            success, result = func(*args)
            if success:
                page.snack_bar = ft.SnackBar(
                    ft.Text(f"✅ PDF généré : {os.path.basename(result)}"),
                    duration=4000
                )
                page.snack_bar.open = True
                page.update()
                try:
                    os.startfile(result)   
                except Exception:
                    pass
            else:
                page.snack_bar = ft.SnackBar(
                    ft.Text(f"❌ Erreur : {result}"),
                    duration=5000
                )
                page.snack_bar.open = True
                page.update()
        threading.Thread(target=run, daemon=True).start()
        
    page.refresh_apprentis = load_apprentis
    
    def return_to_list():
        page.clean()
        page.add(gui_page(page, user_info, on_logout, show_login, show_gui))
        page.update()
    
    
    def show_apprenti_detail(page: ft.Page, apprenti: dict, user_info, show_gui, on_logout, show_login, open_affecter_moyen):

        data = get_apprenti_complet(apprenti["id"])
        if not data:
            page.snack_bar = ft.SnackBar(ft.Text("❌ Impossible de charger les données"))
            page.snack_bar.open = True
            page.update()
            return

        emp = data.get("employeur") or {}
        niveau_scolaire = get_niveau_scolaire_lib(data.get("id_niv"))

        def fmt(d):
            return d.strftime("%d/%m/%Y") if d else ""

        def back(e=None):
            page.clean()
            page.add(gui_page(page, user_info, on_logout, show_login, show_gui))
            page.update()

        def edit_apprenti(e):
            page.clean()
            form = ApprentiForm(
                page,
                apprenti_id=data["id"],
                edit_mode=True,
                on_saved=back,
                on_close=back
            )
            form.show()

        def confirm_delete(e):
            def on_delete(ev):
                success, msg = delete_apprenti(data["id"])
                dlg.open = False
                if success:
                    page.snack_bar = ft.SnackBar(ft.Text(f"✅ {msg}"))
                    back(None)
                else:
                    page.snack_bar = ft.SnackBar(ft.Text(f"❌ {msg}"))
                page.snack_bar.open = True
                page.update()

            def close_dlg(ev):
                dlg.open = False
                page.update()

            dlg = ft.AlertDialog(
                title=ft.Text("Confirmation", color="#EF4444"),
                content=ft.Text(
                    f"Supprimer {data.get('prenom', '')} {data.get('nom', '')} ?"
                ),
                actions=[
                    ft.TextButton("Annuler", on_click=close_dlg),
                    ft.TextButton("Supprimer", on_click=on_delete,
                                style=ft.ButtonStyle(color="#EF4444")),
                ],
            )
            page.dialog = dlg
            dlg.open = True
            page.update()

        page.clean()  
          
        def refresh(e=None):
            show_apprenti_detail(page, apprenti, user_info, show_gui,
                                 on_logout, show_login, open_affecter_moyen)

        def pick_pdf(e: ft.FilePickerResultEvent):
            if not e.files:
                return
            for f in e.files:
                success, msg = save_pdf_apprenti(data["id"], f.path, f.name)
                page.snack_bar = ft.SnackBar(
                    ft.Text(f"✅ {msg}" if success else f"❌ {msg}")
                )
                page.snack_bar.open = True
            refresh()

        file_picker = ft.FilePicker(on_result=pick_pdf)
        page.overlay.append(file_picker)
        page.update()  

        statut = data.get("statut_contrat", "")
        couleur = statut_color(statut)

        header = ft.Row([
            ft.IconButton(ft.icons.ARROW_BACK, icon_color="#20398d", on_click=back),
            ft.Text(
                f"{data.get('code_app', '')} - {data.get('prenom', '')} {data.get('nom', '')}",
                size=20, weight="bold", color="#20398d"
            ),
        
            ft.Container(expand=True),
            ft.Container(
                content=ft.Text(statut, size=12, weight="bold", color="#FFFFFF"),
                bgcolor=couleur,
                border_radius=20,
                padding=ft.Padding(12, 6, 12, 6),
            ),
            ft.IconButton(ft.icons.EDIT, icon_color="#20398d",
                        on_click=edit_apprenti, tooltip="Modifier"),
            ft.IconButton(ft.icons.DELETE, icon_color="#EF4444",
                        on_click=confirm_delete, tooltip="Supprimer"),
        ], vertical_alignment=ft.CrossAxisAlignment.CENTER)

        infos = ft.Column([

            ft.Text("🏢 EMPLOYEUR", size=13, weight="bold", color="#20398d"),
            ft.Row([ft.Text("Type :",              width=160),
                    ft.Text("Privé" if data.get("id_sf") else "Public")]),
            ft.Row([ft.Text("Dénomination :",      width=160),
                    ft.Text(emp.get("denomination", "") or "")]),
            ft.Row([ft.Text("Statut juridique :",  width=160),
                    ft.Text(emp.get("statut", "") or "")]),
            ft.Row([ft.Text("Adresse :",           width=160),
                    ft.Text(emp.get("adresse", "") or "")]),
            ft.Row([ft.Text("Commune :",           width=160),
                    ft.Text(emp.get("commune", "") or "")]),
            ft.Row([ft.Text("Wilaya :",            width=160),
                    ft.Text(emp.get("wilaya", "") or "")]),
            ft.Row([ft.Text("Téléphone :",         width=160),
                    ft.Text(emp.get("telephone", "") or "")]),
            ft.Row([ft.Text("Fax :",               width=160),
                    ft.Text(emp.get("fax", "") or "")]),
            ft.Row([ft.Text("Email :",             width=160),
                    ft.Text(emp.get("email", "") or "")]),

            ft.Divider(),

            ft.Text("📋 APPRENTI", size=13, weight="bold", color="#20398d"),
            ft.Row([ft.Text("Nom :",               width=160),
                    ft.Text(data.get("nom", "") or "")]),
            ft.Row([ft.Text("Prénom :",            width=160),
                    ft.Text(data.get("prenom", "") or "")]),
            ft.Row([ft.Text("Date naissance :",    width=160),
                    ft.Text(fmt(data.get("date_naissance")))]),
            ft.Row([ft.Text("Lieu naissance :",    width=160),
                    ft.Text(data.get("lieu_naissance", "") or "")]),
            ft.Row([ft.Text("Sexe :",              width=160),
                    ft.Text(data.get("sexe", ""))]),
            ft.Row([ft.Text("Groupage :",          width=160),
                    ft.Text(data.get("groupage", ""))]),
            ft.Row([ft.Text("Adresse :",           width=160),
                    ft.Text(data.get("adresse", "") or "")]),
            ft.Row([ft.Text("Commune :",           width=160),
                    ft.Text(data.get("commune", ""))]),
            ft.Row([ft.Text("Wilaya :",            width=160),
                    ft.Text(data.get("wilaya", ""))]),
            ft.Row([ft.Text("Téléphone :",         width=160),
                    ft.Text(data.get("telephone", "") or "")]),
            ft.Row([ft.Text("Email :",             width=160),
                    ft.Text(data.get("email", "") or "")]),
            ft.Row([ft.Text("Niveau scolaire :",   width=160),
                    ft.Text(data.get("niveau_scolaire", ""))]),

            ft.Divider(),

            ft.Text("🏫 ÉTABLISSEMENT DE FORMATION", size=13, weight="bold", color="#20398d"),
            ft.Row([ft.Text("Type :",              width=160),
                    ft.Text(data.get("cf_type", ""))]),
            ft.Row([ft.Text("Adresse :",           width=160),
                    ft.Text(data.get("cf_adresse", ""))]),
            ft.Row([ft.Text("Téléphone :",         width=160),
                    ft.Text(data.get("cf_telephone", ""))]),
            ft.Row([ft.Text("Fax :",               width=160),
                    ft.Text(data.get("cf_fax", ""))]),
            ft.Row([ft.Text("Email :",             width=160),
                    ft.Text(data.get("cf_mail", ""))]),

            ft.Divider(),

            ft.Text("📚 FORMATION", size=13, weight="bold", color="#20398d"),
            ft.Row([ft.Text("Code :",              width=160),
                    ft.Text(data.get("code_sp", ""))]),
            ft.Row([ft.Text("Branche :",           width=160),
                    ft.Text(data.get("branche", ""))]),
            ft.Row([ft.Text("Spécialité :",        width=160),
                    ft.Text(data.get("sous_specialite", ""))]),
            ft.Row([ft.Text("N° inscription :",    width=160),
                    ft.Text((data.get("ninscript", "") or "").strip())]),
            ft.Row([ft.Text("Durée :",             width=160),
                    ft.Text(f"{data.get('duree', '')} mois")]),
            ft.Row([ft.Text("Début :",             width=160),
                    ft.Text(fmt(data.get("date_d")))]),
            ft.Row([ft.Text("Fin :",               width=160),
                    ft.Text(fmt(data.get("date_f")))]),
            ft.Row([ft.Text("Diplôme :",           width=160),
                    ft.Text(data.get("diplome", ""))]),
            ft.Row([ft.Text("Période d'essai :",   width=160),
                    ft.Text(f"{data.get('periode_essai', '')} mois")]),
            ft.Row([ft.Text("Essai du :",          width=160),
                    ft.Text(fmt(data.get("essai_d")))]),
            ft.Row([ft.Text("Essai au :",          width=160),
                    ft.Text(fmt(data.get("essai_f")))]),

            *load_affectations_apprenti(data["id"] , on_refresh= None),

            *([
                ft.Divider(),
                ft.Text("⚠️ RÉSILIATION", size=13, weight="bold", color="#EF4444"),
                ft.Row([ft.Text("Date :",  width=160),
                        ft.Text(fmt(data.get("date_resiliation")))]),
                ft.Row([ft.Text("Motif :", width=160),
                        ft.Text(data.get("motif_resiliation", ""))]),
            ] if data.get("date_resiliation") else []),

            ft.Divider(),
            ft.Text("🔧 MOYENS AFFECTÉS", size=13, weight="bold", color="#20398d"),
            *load_moyens_apprenti(page, data["id"] , on_refresh=None),

            ft.Divider(),
            ft.Row([
                ft.Text("📁 DOSSIER", size=13, weight="bold", color="#20398d"),
                ft.Container(expand=True),
                ft.ElevatedButton(
                    "➕ Ajouter PDF",
                    bgcolor="#20398d",
                    color="#FFFFFF",
                    height=34,
                    on_click=lambda e: file_picker.pick_files(
                        allow_multiple=True,
                        allowed_extensions=["pdf"],
                    ),
                ),
            ], vertical_alignment=ft.CrossAxisAlignment.CENTER),
            *load_pdfs_apprenti(page, data["id"], refresh),

        ], spacing=8, scroll=ft.ScrollMode.AUTO, expand=True)
        photo_src = PhotoManager.get_photo_display_path(data.get("photo_path", ""))

        photo = ft.Container(
            content=ft.Column([
                ft.Container(  
                    content=ft.Image(
                        src=photo_src, 
                        width=150, 
                        height=200, 
                        fit=ft.ImageFit.CONTAIN,  
                        border_radius=8  
                    ),
                    width=150,
                    height=200,
                    border_radius=8,
                    clip_behavior=ft.ClipBehavior.ANTI_ALIAS_WITH_SAVE_LAYER,  
                ),
                ft.ElevatedButton(
                    "Affecter un moyen",
                    width=150,
                    bgcolor="#10B981",
                    on_click=lambda e: open_affecter_moyen(page, data["id"])
                ),
                ft.ElevatedButton(
                    "Affecter à un projet",
                    width=150,
                    bgcolor="#20398d",
                    on_click=lambda e: open_affecter_projet_dialog(page, data["id"])
                ),
                ft.ElevatedButton(
                    "📄 Fiche de charge",
                    width=150,
                    bgcolor="#F59E0B",
                    color="#FFFFFF",
                    on_click=lambda e: _lancer_generation_pdf(
                        page, generer_fiche_individuelle, data["id"]
                    )
                ),
            ], 
            horizontal_alignment=ft.CrossAxisAlignment.CENTER, 
            spacing=10
        ),
        border_radius=8,
        border=ft.border.all(2, "#20398d"),
        padding=10,
        )
        body = ft.Row([
            ft.Container(infos, expand=3),
            ft.Container(width=1, height=400, bgcolor="#E5E7EB"),
            ft.Container(photo, expand=1),
        ], spacing=15, expand=True)

        page.add(
            ft.Container(
                content=ft.Column(
                    [header, ft.Divider(height=2), body],
                    spacing=15, expand=True
                ),
                padding=20, expand=True, bgcolor="#F5F5F7",
            )
        )
        page.update()
 
    def open_affecter_moyen_dialog(page, apprenti_id):
        natures = load_nature_moyens()
        moyens_all = load_moyens()

        selected_nature_id  = {"value": None}
        selected_moyen_id   = {"value": None}
        is_transport_mode   = {"value": False}  

        TRANSPORT_NATURES = {"transport", "gasoil", "prime de panier"}  

        nature_label  = ft.Text("Aucune nature sélectionnée", size=12, color="#6B7280", italic=True)
        nature_list   = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=130)

        moyen_section = ft.Column(visible=True, spacing=6)   
        moyen_label   = ft.Text("Aucun moyen sélectionné", size=12, color="#6B7280", italic=True)
        moyen_list    = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=150)

        qte_field = ft.TextField(
            label="Quantité", width=130, value="1",
            border=ft.InputBorder.OUTLINE, border_radius=10,
        )
        prix_u_field = ft.TextField(
            label="Prix unitaire (DA)", width=180,
            border=ft.InputBorder.OUTLINE, border_radius=10,
            visible=False,  
        )

        search_nature = ft.TextField(
            hint_text="Rechercher une nature...",
            border=ft.InputBorder.OUTLINE, border_radius=10,
            height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
        )
        search_moyen = ft.TextField(
            hint_text="Rechercher un moyen...",
            border=ft.InputBorder.OUTLINE, border_radius=10,
            height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
        )

        def build_nature_list(q=""):
            nature_list.controls = []
            for n in natures:
                label  = n[1] or ""
                is_sel = selected_nature_id["value"] == str(n[0])
                if not label.lower().startswith(q.lower()):
                    continue
                def on_sel(e, nid=str(n[0]), nlbl=label):
                    selected_nature_id["value"] = nid
                    nature_label.value = f"✅ {nlbl}"
                    nature_label.color = "#20398d"

                    mode = nlbl.lower().strip() in TRANSPORT_NATURES
                    is_transport_mode["value"] = mode

                    moyen_section.visible = not mode
                    qte_field.disabled    = mode
                    qte_field.value       = "" if mode else "1"
                    prix_u_field.visible  = mode

                    selected_moyen_id["value"] = None
                    moyen_label.value = "Aucun moyen sélectionné"
                    moyen_label.color = "#6B7280"

                    build_nature_list(search_nature.value or "")
                    build_moyen_list(search_moyen.value or "")
                    nature_label.update()
                    moyen_label.update()
                    nature_list.update()
                    moyen_section.update()
                    qte_field.update()
                    prix_u_field.update()

                nature_list.controls.append(ft.Container(
                    content=ft.Text(label, size=12,
                                    color="#FFFFFF" if is_sel else "#1F2937",
                                    overflow=ft.TextOverflow.ELLIPSIS, max_lines=1),
                    bgcolor="#20398d" if is_sel else "#F9FAFB",
                    border_radius=8,
                    padding=ft.Padding(10, 6, 10, 6),
                    border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                    on_click=on_sel,
                ))

        def build_moyen_list(q=""):
            if not selected_nature_id["value"]:
                moyen_list.controls = [
                    ft.Container(
                        content=ft.Text("Sélectionnez une nature d'abord", size=12, color="#9CA3AF", italic=True),
                        padding=ft.Padding(10, 8, 10, 8),
                    )
                ]
                return
 
            filtered = [
                m for m in moyens_all
                if str(m[3]) == selected_nature_id["value"]
                and f"{m[2]} ({m[1]})".lower().startswith(q.lower())
            ]
            moyen_list.controls = []
            for m in filtered:
                label  = f"{m[2]} ({m[1]})"
                is_sel = selected_moyen_id["value"] == str(m[0])
 
                prix_actuel = get_prix_moyen(m[0])
                prix_str    = f"{prix_actuel:,.2f} DA" if prix_actuel else "— DA"
 
                def on_sel(e, mid=str(m[0]), lbl=label, prix=prix_actuel):
                    selected_moyen_id["value"] = mid
                    moyen_label.value = f"✅ {lbl}"
                    moyen_label.color = "#20398d"
                    prix_u_field.value   = str(prix) if prix else ""
                    prix_u_field.visible = True
                    build_moyen_list(search_moyen.value or "")
                    moyen_label.update()
                    moyen_list.update()
                    prix_u_field.update()
 
                moyen_list.controls.append(ft.Container(
                    content=ft.Row([
                        ft.Column([
                            ft.Text(m[2], size=12,
                                    color="#FFFFFF" if is_sel else "#1F2937",
                                    overflow=ft.TextOverflow.ELLIPSIS, max_lines=1,
                                    weight="w500"),
                            ft.Text(f"Code : {m[1]}", size=10,
                                    color="#E0E7FF" if is_sel else "#9CA3AF"),
                        ], spacing=1, expand=True),
                        ft.Container(
                            content=ft.Text(
                                prix_str, size=11,
                                color="#FFFFFF" if is_sel else "#10B981",
                                italic=True, weight="bold",
                            ),
                            bgcolor="#1e2e6e" if is_sel else "#ECFDF5",
                            border_radius=6,
                            padding=ft.Padding(6, 3, 6, 3),
                        ),
                    ], vertical_alignment=ft.CrossAxisAlignment.CENTER),
                    bgcolor="#20398d" if is_sel else "#F9FAFB",
                    border_radius=8,
                    padding=ft.Padding(10, 6, 10, 6),
                    border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                    on_click=on_sel,
                ))
        search_nature.on_change = lambda e: (build_nature_list(e.control.value), nature_list.update())
        search_moyen.on_change  = lambda e: (build_moyen_list(e.control.value),  moyen_list.update())

        build_nature_list()
        build_moyen_list()

        moyen_section.controls = [
            ft.Text("Moyen *", size=13, weight="bold", color="#20398d"),
            search_moyen,
            moyen_label,
            moyen_list,
        ]
        def _get_id_affectation(cursor, apprenti_id):
            print(f"[DEBUG] Recherche affectation pour apprenti_id={apprenti_id} (type={type(apprenti_id)})")
            try:
                cursor.execute("SELECT TOP 1 ID FROM AFFECTATION WHERE ID_APP = ? ORDER BY ID DESC", (apprenti_id,))
                row = cursor.fetchone()
                if row:
                    print(f"[DEBUG] Trouvé via ID_APP : id_af={row[0]}")
                    return row[0]
            except Exception as ex:
                print(f"[DEBUG] ID_APP échoué : {ex}")
            try:
                cursor.execute("SELECT TOP 1 ID FROM AFFECTATION WHERE ID_APPRENTI = ? ORDER BY ID DESC", (apprenti_id,))
                row = cursor.fetchone()
                if row:
                    print(f"[DEBUG] Trouvé via ID_APPRENTI : id_af={row[0]}")
                    return row[0]
            except Exception as ex:
                print(f"[DEBUG] ID_APPRENTI échoué : {ex}")
            try:
                cursor.execute("SELECT TOP 1 * FROM AFFECTATION")
                cols = [c[0] for c in cursor.description]
                print(f"[DEBUG] Colonnes AFFECTATION : {cols}")
                cursor.execute("SELECT COUNT(*) FROM AFFECTATION")
                print(f"[DEBUG] Total lignes AFFECTATION : {cursor.fetchone()[0]}")
            except Exception as ex:
                print(f"[DEBUG] Impossible de lire AFFECTATION : {ex}")
            return None
 
        def valider(e):
            if not selected_nature_id["value"]:
                page.snack_bar = ft.SnackBar(ft.Text("❌ Sélectionnez une nature de moyen"))
                page.snack_bar.open = True
                page.update()
                return

            mode_transport = is_transport_mode["value"]

            prix_val = None
            if prix_u_field.value and prix_u_field.value.strip():
                try:
                    prix_val = float(prix_u_field.value.strip())
                except ValueError:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Prix unitaire invalide"))
                    page.snack_bar.open = True
                    page.update()
                    return

            if mode_transport:
                if prix_val is None:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Saisissez le prix unitaire"))
                    page.snack_bar.open = True
                    page.update()
                    return
                _save_transport(prix_val)
            else:
                if not selected_moyen_id["value"]:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Sélectionnez un moyen"))
                    page.snack_bar.open = True
                    page.update()
                    return
                qte = qte_field.value.strip()
                if not qte.isdigit() or int(qte) <= 0:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Quantité invalide"))
                    page.snack_bar.open = True
                    page.update()
                    return
                _save_moyen_normal(int(qte), prix_val)

        def _save_transport(prix_u):
            conn = DatabaseConfig.get_connection()
            if not conn:
                return
            try:
                cursor = conn.cursor()
                cursor.execute("""
                               SELECT TOP 1 ID FROM AFFECTATION WHERE ID_APP = ? ORDER BY ID DESC
                """, (apprenti_id,))
                row = cursor.fetchone()
                if not row:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Aucune affectation projet trouvée"))
                    page.snack_bar.open = True
                    page.update()
                    return
                id_af = row[0]

                cursor.execute("""
                    INSERT INTO [AFFECTATION MOYEN]
                        (ID_AF, ID_NMY, ID_MY, QUANTITE, PRIX_U, DATE_MY)
                    VALUES (?, ?, NULL, 1, ?, GETDATE())
                """, (id_af, int(selected_nature_id["value"]), prix_u))

                conn.commit()
                dialog.open = False
                page.snack_bar = ft.SnackBar(ft.Text("✅ Affectation enregistrée"))
                page.snack_bar.open = True
                page.update()
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {str(ex)[:100]}"))
                page.snack_bar.open = True
                page.update()
            finally:
                conn.close()

        def _save_moyen_normal(qte, prix_u):
            conn = DatabaseConfig.get_connection()
            if not conn:
                return
            try:
                cursor = conn.cursor()
 
                moyen_id = int(selected_moyen_id["value"])
 
                id_af = _get_id_affectation(cursor, apprenti_id)
                if not id_af:
                    page.snack_bar = ft.SnackBar(ft.Text(
                        "❌ Aucune affectation projet trouvée. Veuillez d'abord affecter l'apprenti à un projet."
                    ))
                    page.snack_bar.open = True
                    page.update()
                    return
 
                cursor.execute("""
                    SELECT ID_NMY, CODE, DESIGNATION, [U.M], DPA_HT, DPA_TTC, TVA, NUM_FACTURE
                    FROM MOYEN WHERE ID = ?
                """, (moyen_id,))
                moyen_row = cursor.fetchone()
                id_nmy = moyen_row[0] if moyen_row else int(selected_nature_id["value"])
 
                if prix_u is not None and moyen_row:
                    tva    = moyen_row[6]
                    dpa_ht = round(prix_u / (1 + tva / 100), 2) if tva else None
 
                    cursor.execute("""
                        INSERT INTO MOYEN
                            (CODE, DESIGNATION, [U.M], DPA_HT, DPA_TTC, TVA, NUM_FACTURE, ID_NMY, DATE_FACTURE)
                        OUTPUT INSERTED.ID
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(GETDATE() AS DATE))
                    """, (
                        moyen_row[1], 
                        moyen_row[2],  
                        moyen_row[3],  
                        dpa_ht,       
                        prix_u,        
                        tva,          
                        moyen_row[7],  
                        id_nmy,       
                    ))
                    new_row = cursor.fetchone()
                    if new_row:
                        moyen_id = new_row[0]  
 
                cursor.execute("""
                    INSERT INTO [AFFECTATION MOYEN]
                        (ID_AF, ID_NMY, ID_MY, QUANTITE, PRIX_U, DATE_MY)
                    VALUES (?, ?, ?, ?, ?, GETDATE())
                """, (id_af, id_nmy, moyen_id, qte, prix_u))
 
                conn.commit()
                dialog.open = False
                page.snack_bar = ft.SnackBar(ft.Text("✅ Moyen affecté avec succès"))
                page.snack_bar.open = True
                page.update()
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {str(ex)[:100]}"))
                page.snack_bar.open = True
                page.update()
            finally:
                conn.close()
        dialog = ft.AlertDialog(
            title=ft.Text("Affecter un moyen à l'apprenti", color="#20398d", weight="bold"),
            content=ft.Container(
                width=460, height=560,
                content=ft.Column([
                    ft.Text("Nature du moyen *", size=13, weight="bold", color="#20398d"),
                    search_nature,
                    nature_label,
                    nature_list,
                    ft.Divider(),
                    moyen_section,
                    ft.Row([
                        qte_field,
                        prix_u_field,
                        ft.ElevatedButton(
                            "Nouveau moyen",
                            icon=ft.icons.ADD,
                            visible=True,
                            on_click=lambda e: open_new_moyen_dialog(page),
                        ),
                    ], spacing=10),
                ], spacing=8, scroll=ft.ScrollMode.AUTO),
            ),
            actions=[
                ft.TextButton("Annuler",
                            on_click=lambda e: close_dialog(page, dialog)),
                ft.ElevatedButton("Valider", bgcolor="#20398d",
                                on_click=valider),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        dialog.open = True
        page.dialog = dialog
        page.update()

    def affecter_moyen(page, apprenti_id, moyen_id, quantite):
        try:
            if not moyen_id:
                page.snack_bar = ft.SnackBar(ft.Text("❌ Veuillez sélectionner un moyen"))
                page.snack_bar.open = True
                page.update()
                return

            if not quantite or not str(quantite).strip().isdigit() or int(quantite) <= 0:
                page.snack_bar = ft.SnackBar(ft.Text("❌ Quantité invalide"))
                page.snack_bar.open = True
                page.update()
                return

            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT TOP 1 ID FROM AFFECTATION WHERE ID_APP = ? ORDER BY ID DESC

            """, (apprenti_id,))
            row = cursor.fetchone()
            if not row:
                page.snack_bar = ft.SnackBar(
                    ft.Text("❌ Aucune affectation trouvée pour cet apprenti")
                )
                page.snack_bar.open = True
                page.update()
                conn.close()
                return
            id_af = row[0]

            cursor.execute("""
                SELECT ID_NMY
                FROM MOYEN
                WHERE ID = ?
            """, (moyen_id,))
            row = cursor.fetchone()
            if not row:
                page.snack_bar = ft.SnackBar(ft.Text("❌ Moyen introuvable en base"))
                page.snack_bar.open = True
                page.update()
                conn.close()
                return
            id_nmy = row[0]  
            cursor.execute("""
                INSERT INTO [AFFECTATION MOYEN]
                    (ID_AF, ID_NMY, ID_MY, QUANTITE, DATE_MY)
                VALUES
                    (?,     ?,      ?,     ?,         GETDATE())
            """, (
                id_af,      
                id_nmy,     
                moyen_id,   
                int(quantite)
            ))

            conn.commit()
            conn.close()

            page.snack_bar = ft.SnackBar(ft.Text("✅ Moyen affecté avec succès"))
            page.snack_bar.open = True
            page.update()

        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {ex}"))
            page.snack_bar.open = True
            page.update()

    def open_new_moyen_dialog(page):
        new_code = ft.TextField(label="Code")
        new_designation = ft.TextField(label="Désignation")
        new_um = ft.TextField(label="Unité (U.M)")
        new_dpa_ht = ft.TextField(label="DPA HT")
        new_dpa_ttc = ft.TextField(label="DPA TTC")
        new_tva = ft.TextField(label="TVA")
        new_num_facture = ft.TextField(label="N° Facture")
        new_methode_calcul = ft.TextField(label="Méthode de calcul")
        new_date_facture = ft.TextField(label="Date de Facture")


        natures = load_nature_moyens()
        nature_dd = ft.Dropdown(
            label="Nature du moyen *",
            options=[
                ft.dropdown.Option(key=str(n[0]), text=n[1])
                for n in natures
            ]
        )
        methode = load_methode_calcul()
        methode_l = ft.Dropdown(
            label="Méthode de calcul *",
            value=str(methode[0][0]) if methode else None,
            options=[
                ft.dropdown.Option(key=str(m[0]), text=str(m[1] or ""))
                for m in methode
            ]
        )

        dialog = ft.AlertDialog(
            title=ft.Text("Ajouter un nouveau moyen"),
            content=ft.Column(
                [
                    new_code,
                    new_designation,
                    nature_dd,
                    new_um,
                    new_dpa_ht,
                    new_dpa_ttc,
                    new_tva,
                    new_num_facture,
                    new_methode_calcul,
                    new_date_facture
                ],
                spacing=10,
                scroll=ft.ScrollMode.AUTO,
            ),
            actions=[
                ft.TextButton("Annuler", on_click=lambda e: close_dialog(page, dialog)),
                ft.ElevatedButton(
                    "Ajouter",
                    on_click=lambda e: add_new_moyen(
                        page,
                        new_code.value,
                        new_designation.value,
                        nature_dd.value,
                        new_um.value,
                        new_dpa_ht.value,
                        new_dpa_ttc.value,
                        new_tva.value,
                        new_num_facture.value,
                        new_methode_calcul.value,
                        new_date_facture.value,
                        dialog,
                    ),
                ),
            ],
        )

        dialog.open = True
        page.dialog = dialog
        page.update()
    def add_new_moyen(page, code, designation, id_nmy, u_m,
                      dpa_ht, dpa_ttc, tva, num_facture,
                      methode_calcul, date_facture, dialog):
        if not code or not designation or not id_nmy:
            page.snack_bar = ft.SnackBar(
                ft.Text("Code, désignation et nature sont obligatoires")
            )
            page.snack_bar.open = True
            page.update()
            return

        conn = DatabaseConfig.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO MOYEN
            (CODE, DESIGNATION, [U.M], DPA_HT, DPA_TTC, TVA, NUM_FACTURE, ID_NMY)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            code,
            designation,
            u_m,
            dpa_ht or None,
            dpa_ttc or None,
            tva or None,
            num_facture,
            int(id_nmy),
        ))
        conn.commit()
        conn.close()
        dialog.open = False
        page.snack_bar = ft.SnackBar(ft.Text("✅ Moyen ajouté avec succès"))
        page.snack_bar.open = True
        page.update()

    def close_dialog(page, dialog):
        dialog.open = False
        page.update()
    def load_projets() -> list:
        conn = DatabaseConfig.get_connection()
        if not conn:
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT ID, LIB_PROJET, ADRESSE FROM CENTRE_DE_COUT ORDER BY LIB_PROJET")
            return cursor.fetchall()
        except Exception as e:
            print(f"Erreur chargement projets : {e}")
            return []
        finally:
            conn.close()
    maitres = load_maitres()

    maitre_dropdown = ft.Dropdown(
        label="Maître d'apprentissage",
        width=350,
        options=[
            ft.dropdown.Option(
                key=str(m[0]),
                text=f"{m[1]} {m[2]}"
            )
            for m in maitres
        ]
    )
    def open_modifier_affectation_projet_dialog(page, id_affectation, on_refresh):
        conn = DatabaseConfig.get_connection()
        if not conn:
            return
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    AF.ID_PROJET, AF.ID_MA,
                    AF.DATE_D, AF.DATE_F, AF.DATE_DR, AF.DATE_FR
                FROM AFFECTATION AF
                WHERE AF.ID = ?
            """, (id_affectation,))
            row = cursor.fetchone()
            if not row:
                return

            projets      = load_projets()
            maitres_list = load_maitres()

            selected_projet_id = {"value": str(row[0]) if row[0] else None}
            selected_maitre_id = {"value": str(row[1]) if row[1] else None}

            def fmt(d):
                return d.strftime("%d/%m/%Y") if d else ""

            projet_label = ft.Text(
                next((f"✅ {p[1]}" for p in projets if str(p[0]) == selected_projet_id["value"]), "—"),
                size=12, color="#20398d"
            )
            maitre_label = ft.Text(
                next((f"✅ {m[2]} {m[1]}" for m in maitres_list if str(m[0]) == selected_maitre_id["value"]), "—"),
                size=12, color="#20398d"
            )

            projet_list = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=120)
            maitre_list_col = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=100)

            def build_projet_list(q=""):
                projet_list.controls = []
                for p in projets:
                    label  = p[1] or ""
                    is_sel = selected_projet_id["value"] == str(p[0])
                    if not label.lower().startswith(q.lower()):
                        continue
                    def on_sel(e, pid=str(p[0]), lbl=label):
                        selected_projet_id["value"] = pid
                        projet_label.value = f"✅ {lbl}"
                        projet_label.color = "#20398d"
                        build_projet_list(search_projet.value or "")
                        projet_label.update()
                        projet_list.update()
                    projet_list.controls.append(ft.Container(
                        content=ft.Text(label, size=12,
                                        color="#FFFFFF" if is_sel else "#1F2937",
                                        overflow=ft.TextOverflow.ELLIPSIS, max_lines=1),
                        bgcolor="#20398d" if is_sel else "#F9FAFB",
                        border_radius=8,
                        padding=ft.Padding(10, 6, 10, 6),
                        border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                        on_click=on_sel,
                    ))

            def build_maitre_list(q=""):
                maitre_list_col.controls = []
                for m in maitres_list:
                    label  = f"{m[2]} {m[1]}"
                    is_sel = selected_maitre_id["value"] == str(m[0])
                    if not label.lower().startswith(q.lower()):
                        continue
                    def on_sel(e, mid=str(m[0]), lbl=label):
                        selected_maitre_id["value"] = mid
                        maitre_label.value = f"✅ {lbl}"
                        maitre_label.color = "#20398d"
                        build_maitre_list(search_maitre.value or "")
                        maitre_label.update()
                        maitre_list_col.update()
                    maitre_list_col.controls.append(ft.Container(
                        content=ft.Text(label, size=12,
                                        color="#FFFFFF" if is_sel else "#1F2937",
                                        overflow=ft.TextOverflow.ELLIPSIS, max_lines=1),
                        bgcolor="#20398d" if is_sel else "#F9FAFB",
                        border_radius=8,
                        padding=ft.Padding(10, 6, 10, 6),
                        border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                        on_click=on_sel,
                    ))

            search_projet = ft.TextField(
                hint_text="Rechercher un projet...",
                border=ft.InputBorder.OUTLINE, border_radius=10,
                height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
                on_change=lambda e: (build_projet_list(e.control.value), projet_list.update()),
            )
            search_maitre = ft.TextField(
                hint_text="Rechercher un maître...",
                border=ft.InputBorder.OUTLINE, border_radius=10,
                height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
                on_change=lambda e: (build_maitre_list(e.control.value), maitre_list_col.update()),
            )

            build_projet_list()
            build_maitre_list()

            date_debut      = ft.TextField(label="Date début",  width=150, read_only=True,
                                        value=fmt(row[2]), suffix_icon=ft.icons.CALENDAR_TODAY)
            date_fin        = ft.TextField(label="Date fin",    width=150, read_only=True,
                                        value=fmt(row[3]), suffix_icon=ft.icons.CALENDAR_TODAY)
            date_debut_reel = ft.TextField(label="Début réel",  width=150, read_only=True,
                                        value=fmt(row[4]), suffix_icon=ft.icons.CALENDAR_TODAY)
            date_fin_reel   = ft.TextField(label="Fin réelle",  width=150, read_only=True,
                                        value=fmt(row[5]), suffix_icon=ft.icons.CALENDAR_TODAY)

            date_debut_picker      = ft.DatePicker(on_change=lambda e: (setattr(date_debut,      'value', date_debut_picker.value.strftime("%d/%m/%Y")),      page.update()))
            date_fin_picker        = ft.DatePicker(on_change=lambda e: (setattr(date_fin,        'value', date_fin_picker.value.strftime("%d/%m/%Y")),        page.update()))
            date_debut_reel_picker = ft.DatePicker(on_change=lambda e: (setattr(date_debut_reel, 'value', date_debut_reel_picker.value.strftime("%d/%m/%Y")), page.update()))
            date_fin_reel_picker   = ft.DatePicker(on_change=lambda e: (setattr(date_fin_reel,   'value', date_fin_reel_picker.value.strftime("%d/%m/%Y")),   page.update()))

            page.overlay.extend([date_debut_picker, date_fin_picker,
                                date_debut_reel_picker, date_fin_reel_picker])

            def save_modifications(e):
                if not selected_projet_id["value"]:
                    page.snack_bar = ft.SnackBar(ft.Text("❌ Sélectionnez un projet"))
                    page.snack_bar.open = True
                    page.update()
                    return

                conn2 = DatabaseConfig.get_connection()
                if not conn2:
                    return
                try:
                    cursor2 = conn2.cursor()

                    ancien_maitre_id = str(row[1]) if row[1] else None
                    nouveau_maitre_id = selected_maitre_id["value"]

                    if nouveau_maitre_id and nouveau_maitre_id != ancien_maitre_id:
                        cursor2.execute("""
                            INSERT INTO HISTORIQUE_MAITRE (ID_AFFECTATION, ID_MA_ANCIEN, ID_MA_NOUVEAU)
                            VALUES (?, ?, ?)
                        """, (
                            id_affectation,
                            int(ancien_maitre_id) if ancien_maitre_id else None,
                            int(nouveau_maitre_id)
                        ))

                    def parse_date(s):
                        return datetime.strptime(s, "%d/%m/%Y").date() if s else None

                    cursor2.execute("""
                        UPDATE AFFECTATION SET
                            ID_PROJET = ?,
                            ID_MA     = ?,
                            DATE_D    = ?,
                            DATE_F    = ?,
                            DATE_DR   = ?,
                            DATE_FR   = ?
                        WHERE ID = ?
                    """, (
                        int(selected_projet_id["value"]),
                        int(nouveau_maitre_id) if nouveau_maitre_id else None,
                        parse_date(date_debut.value),
                        parse_date(date_fin.value),
                        parse_date(date_debut_reel.value),
                        parse_date(date_fin_reel.value),
                        id_affectation,
                    ))
                    conn2.commit()
                    dialog.open = False
                    page.snack_bar = ft.SnackBar(ft.Text("✅ Affectation projet modifiée"))
                    page.snack_bar.open = True
                    on_refresh()
                except Exception as ex:
                    page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {str(ex)[:100]}"))
                    page.snack_bar.open = True
                    page.update()
                finally:
                    conn2.close()

            dialog = ft.AlertDialog(
                title=ft.Text("✏️ Modifier l'affectation projet",
                            color="#20398d", weight="bold"),
                content=ft.Container(
                    width=450, height=580,
                    content=ft.Column([
                        ft.Text("Projet *", size=13, weight="bold", color="#20398d"),
                        search_projet, projet_label, projet_list,
                        ft.Divider(),
                        ft.Row([
                            ft.Container(content=date_debut,
                                        on_click=lambda e: date_debut_picker.pick_date()),
                            ft.Container(content=date_fin,
                                        on_click=lambda e: date_fin_picker.pick_date()),
                        ], spacing=10),
                        ft.Row([
                            ft.Container(content=date_debut_reel,
                                        on_click=lambda e: date_debut_reel_picker.pick_date()),
                            ft.Container(content=date_fin_reel,
                                        on_click=lambda e: date_fin_reel_picker.pick_date()),
                        ], spacing=10),
                        ft.Divider(),
                        ft.Text("Maître d'apprentissage", size=13,
                                weight="bold", color="#20398d"),
                        search_maitre, maitre_label, maitre_list_col,
                    ], spacing=8, scroll=ft.ScrollMode.AUTO),
                ),
                actions=[
                    ft.TextButton("Annuler",
                                on_click=lambda e: (setattr(dialog, 'open', False), page.update())),
                    ft.ElevatedButton("💾 Enregistrer", bgcolor="#20398d",
                                    on_click=save_modifications),
                ],
                actions_alignment=ft.MainAxisAlignment.END,
            )
            dialog.open = True
            page.dialog = dialog
            page.update()

        except Exception as e:
            print(f"Erreur open_modifier_affectation: {e}")
        finally:
            conn.close()

    def open_affecter_projet_dialog(page, apprenti_id):
        projets       = load_projets()
        maitres_list  = load_maitres()

        selected_projet_id = {"value": None}
        selected_maitre_id = {"value": None}

        projet_label = ft.Text("Aucun projet sélectionné", size=12, color="#6B7280", italic=True)
        maitre_label = ft.Text("Aucun maître sélectionné", size=12, color="#6B7280", italic=True)

        projet_list = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=130)
        maitre_list = ft.Column(spacing=3, scroll=ft.ScrollMode.AUTO, height=110)

        def build_projet_list(q=""):
            projet_list.controls = []
            for p in projets:
                label = p[1] or ""
                if not label.lower().startswith(q.lower()):
                    continue
                is_sel = selected_projet_id["value"] == str(p[0])
                def on_sel(e, pid=str(p[0]), lbl=label):
                    selected_projet_id["value"] = pid
                    projet_label.value = f"✅ {lbl}"
                    projet_label.color = "#20398d"
                    build_projet_list(search_projet.value or "")
                    projet_label.update()
                    projet_list.update()
                projet_list.controls.append(ft.Container(
                    content=ft.Text(label, size=12,
                                    color="#FFFFFF" if is_sel else "#1F2937",
                                    overflow=ft.TextOverflow.ELLIPSIS, max_lines=1),
                    bgcolor="#20398d" if is_sel else "#F9FAFB",
                    border_radius=8,
                    padding=ft.Padding(10, 6, 10, 6),
                    border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                    on_click=on_sel,
                ))

        def build_maitre_list(q=""):
            maitre_list.controls = []
            for m in maitres_list:
                label = f"{m[1]} {m[2]}"
                if not label.lower().startswith(q.lower()):
                    continue
                is_sel = selected_maitre_id["value"] == str(m[0])
                def on_sel(e, mid=str(m[0]), lbl=label):
                    selected_maitre_id["value"] = mid
                    maitre_label.value = f"✅ {lbl}"
                    maitre_label.color = "#20398d"
                    build_maitre_list(search_maitre.value or "")
                    maitre_label.update()
                    maitre_list.update()
                maitre_list.controls.append(ft.Container(
                    content=ft.Text(label, size=12,
                                    color="#FFFFFF" if is_sel else "#1F2937",
                                    overflow=ft.TextOverflow.ELLIPSIS, max_lines=1),
                    bgcolor="#20398d" if is_sel else "#F9FAFB",
                    border_radius=8,
                    padding=ft.Padding(10, 6, 10, 6),
                    border=ft.border.all(1, "#20398d" if is_sel else "#E5E7EB"),
                    on_click=on_sel,
                ))

        search_projet = ft.TextField(
            hint_text="Rechercher un projet...",
            border=ft.InputBorder.OUTLINE, border_radius=10,
            height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
            on_change=lambda e: (build_projet_list(e.control.value), projet_list.update()),
        )
        search_maitre = ft.TextField(
            hint_text="Rechercher un maître...",
            border=ft.InputBorder.OUTLINE, border_radius=10,
            height=42, text_size=13, prefix_icon=ft.icons.SEARCH,
            on_change=lambda e: (build_maitre_list(e.control.value), maitre_list.update()),
        )

        build_projet_list()
        build_maitre_list()

        date_debut      = ft.TextField(label="Date début",   width=150, read_only=True, suffix_icon=ft.icons.CALENDAR_TODAY)
        date_fin        = ft.TextField(label="Date fin",     width=150, read_only=True, suffix_icon=ft.icons.CALENDAR_TODAY)
        date_debut_reel = ft.TextField(label="Début réel",   width=150, read_only=True, suffix_icon=ft.icons.CALENDAR_TODAY)
        date_fin_reel   = ft.TextField(label="Fin réelle",   width=150, read_only=True, suffix_icon=ft.icons.CALENDAR_TODAY)

        date_debut_picker      = ft.DatePicker(on_change=lambda e: (setattr(date_debut,      'value', date_debut_picker.value.strftime("%d/%m/%Y")),      page.update()))
        date_fin_picker        = ft.DatePicker(on_change=lambda e: (setattr(date_fin,        'value', date_fin_picker.value.strftime("%d/%m/%Y")),        page.update()))
        date_debut_reel_picker = ft.DatePicker(on_change=lambda e: (setattr(date_debut_reel, 'value', date_debut_reel_picker.value.strftime("%d/%m/%Y")), page.update()))
        date_fin_reel_picker   = ft.DatePicker(on_change=lambda e: (setattr(date_fin_reel,   'value', date_fin_reel_picker.value.strftime("%d/%m/%Y")),   page.update()))

        page.overlay.extend([date_debut_picker, date_fin_picker,
                              date_debut_reel_picker, date_fin_reel_picker])

        dialog = ft.AlertDialog(
            title=ft.Text("Affecter à un projet", color="#20398d", weight="bold"),
            content=ft.Container(
                width=440, height=580,
                content=ft.Column([
                    ft.Text("Projet *", size=13, weight="bold", color="#20398d"),
                    search_projet,
                    projet_label,
                    projet_list,
                    ft.Divider(),
                    ft.Row([
                        ft.Container(content=date_debut,
                                     on_click=lambda e: date_debut_picker.pick_date()),
                        ft.Container(content=date_fin,
                                     on_click=lambda e: date_fin_picker.pick_date()),
                    ], spacing=10),
                    ft.Row([
                        ft.Container(content=date_debut_reel,
                                     on_click=lambda e: date_debut_reel_picker.pick_date()),
                        ft.Container(content=date_fin_reel,
                                     on_click=lambda e: date_fin_reel_picker.pick_date()),
                    ], spacing=10),
                    ft.Divider(),
                    ft.Text("Maître d'apprentissage", size=13,
                            weight="bold", color="#20398d"),
                    search_maitre,
                    maitre_label,
                    maitre_list,
                ], spacing=8, scroll=ft.ScrollMode.AUTO),
            ),
            actions=[
                ft.TextButton("Annuler",
                              on_click=lambda e: close_dialog(page, dialog)),
                ft.ElevatedButton(
                    "Valider", bgcolor="#20398d",
                    on_click=lambda e: save_affectation_projet(
                        page, apprenti_id,
                        selected_projet_id["value"],
                        date_debut.value, date_fin.value,
                        date_debut_reel.value, date_fin_reel.value,
                        selected_maitre_id["value"],
                        dialog,
                    )
                ),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        dialog.open = True
        page.dialog = dialog
        page.update()
    def save_affectation_projet(page, apprenti_id, projet_id,
                            date_d_str, date_f_str,
                            date_dr_str, date_fr_str,
                            maitre_id,
                            dialog):       
        if not projet_id:
            page.snack_bar = ft.SnackBar(ft.Text("❌ Veuillez sélectionner un projet"))
            page.snack_bar.open = True
            page.update()
            return

        conn = DatabaseConfig.get_connection()
        if not conn:
            page.snack_bar = ft.SnackBar(ft.Text("❌ Erreur connexion BD"))
            page.snack_bar.open = True
            page.update()
            return
        try:
            cursor = conn.cursor()

            date_d = datetime.strptime(date_d_str, "%d/%m/%Y").date() if date_d_str else None
            date_f = datetime.strptime(date_f_str, "%d/%m/%Y").date() if date_f_str else None
            date_dr = datetime.strptime(date_dr_str, "%d/%m/%Y").date() if date_dr_str else None
            date_fr = datetime.strptime(date_fr_str, "%d/%m/%Y").date() if date_fr_str else None

            cursor.execute("SELECT ID_SP FROM APPRENTIE WHERE ID = ?", (apprenti_id,))
            row = cursor.fetchone()
            id_sp = row[0] if row else None
            cursor.execute("""
                SELECT ISNULL(MAX(NUM_AFF), 0) + 1
                FROM AFFECTATION
                WHERE ID_APP = ?
            """, (apprenti_id,))

            num_aff = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO AFFECTATION (
                    ID_APP,
                    ID_PROJET,
                    ID_SP,
                    ID_MA,
                    DATE_D,
                    DATE_DR,
                    DATE_F,
                    DATE_FR,
                    NUM_AFF
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                apprenti_id,
                int(projet_id),
                id_sp,
                int(maitre_id) if maitre_id else None,
                date_d,
                date_dr,
                date_f,
                date_fr,
                num_aff,
            ))

            conn.commit()
            dialog.open = False
            page.snack_bar = ft.SnackBar(ft.Text("✅ Affectation projet enregistrée"))
            page.snack_bar.open = True
            page.update()

        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {str(ex)[:100]}"))
            page.snack_bar.open = True
            page.update()
        finally:
            conn.close()

    def changer_maitre_affectation(page, id_affectation, nouveau_maitre_id, dialog=None):
        conn = DatabaseConfig.get_connection()
        if not conn:
            page.snack_bar = ft.SnackBar(ft.Text("❌ Erreur connexion BD"))
            page.snack_bar.open = True
            page.update()
            return False
        try:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT ID_MA FROM AFFECTATION WHERE ID = ?",
                (id_affectation,)
            )
            row = cursor.fetchone()
            if not row:
                page.snack_bar = ft.SnackBar(ft.Text("❌ Affectation introuvable"))
                page.snack_bar.open = True
                page.update()
                return False

            ancien_maitre_id = row[0]  

            cursor.execute("""
                INSERT INTO HISTORIQUE_MAITRE (ID_AFFECTATION, ID_MA_ANCIEN, ID_MA_NOUVEAU)
                VALUES (?, ?, ?)
            """, (id_affectation, ancien_maitre_id, int(nouveau_maitre_id)))

            cursor.execute(
                "UPDATE AFFECTATION SET ID_MA = ? WHERE ID = ?",
                (int(nouveau_maitre_id), id_affectation)
            )

            conn.commit()

            ancien_nom   = ""
            nouveau_nom  = ""

            if ancien_maitre_id:
                cursor.execute(
                    "SELECT NOM, PRENOM FROM MAITREAPPRENTISSAGE WHERE ID = ?",
                    (ancien_maitre_id,)
                )
                m = cursor.fetchone()
                if m:
                    ancien_nom = f"{m[1]} {m[0]}"

            cursor.execute(
                "SELECT NOM, PRENOM FROM MAITREAPPRENTISSAGE WHERE ID = ?",
                (int(nouveau_maitre_id),)
            )
            m = cursor.fetchone()
            if m:
                nouveau_nom = f"{m[1]} {m[0]}"

            msg = f"✅ Maître changé"
            if ancien_nom:
                msg += f" : {ancien_nom} → {nouveau_nom}"
            else:
                msg += f" : {nouveau_nom} assigné"

            if dialog:
                dialog.open = False

            page.snack_bar = ft.SnackBar(ft.Text(msg))
            page.snack_bar.open = True
            page.update()
            return True

        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"❌ Erreur : {str(ex)[:100]}"))
            page.snack_bar.open = True
            page.update()
            return False
        finally:
            conn.close()
    def get_historique_maitre(id_affectation: int) -> list:
        conn = DatabaseConfig.get_connection()
        if not conn:
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    H.DATE_CHANGEMENT,
                    MA_ANC.NOM      AS ANC_NOM,
                    MA_ANC.PRENOM   AS ANC_PRENOM,
                    MA_NV.NOM       AS NV_NOM,
                    MA_NV.PRENOM    AS NV_PRENOM
                FROM HISTORIQUE_MAITRE H
                LEFT JOIN MAITREAPPRENTISSAGE MA_ANC ON H.ID_MA_ANCIEN  = MA_ANC.ID
                LEFT JOIN MAITREAPPRENTISSAGE MA_NV  ON H.ID_MA_NOUVEAU = MA_NV.ID
                WHERE H.ID_AFFECTATION = ?
                ORDER BY H.DATE_CHANGEMENT DESC
            """, (id_affectation,))
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            print(f"Erreur historique maître : {e}")
            return []
        finally:
            conn.close()
    def load_affectations_apprenti(apprenti_id: int , on_refresh) -> list:
        conn = DatabaseConfig.get_connection()
        if not conn:
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    AF.ID,
                    AF.NUM_AFF,
                    CC.LIB_PROJET,
                    CC.ADRESSE,
                    AF.DATE_D,
                    AF.DATE_F,
                    AF.DATE_DR,
                    AF.DATE_FR,
                    MA.NOM    AS MAITRE_NOM,
                    MA.PRENOM AS MAITRE_PRENOM
                FROM AFFECTATION AF
                LEFT JOIN CENTRE_DE_COUT      CC ON AF.ID_PROJET = CC.ID
                LEFT JOIN MAITREAPPRENTISSAGE MA ON AF.ID_MA     = MA.ID
                WHERE AF.ID_APP = ?
                ORDER BY AF.DATE_D DESC
            """, (apprenti_id,))
            rows = cursor.fetchall()

            if not rows:
                return []

            def fmt(d):
                return d.strftime("%d/%m/%Y") if d else ""

            def fmt_str(s):
                return s.strftime("%d/%m/%Y %H:%M") if s else ""

            controls = [
                ft.Divider(),
                ft.Text("🏗️ AFFECTATIONS PROJETS", size=13,
                        weight="bold", color="#20398d"),
            ]

            for r in rows:
                id_affectation = r[0]
                maitre_nom     = (r[8] or "").strip()
                maitre_prenom  = (r[9] or "").strip()
                maitre_actuel  = f"{maitre_prenom} {maitre_nom}".strip() \
                                if (maitre_nom or maitre_prenom) else "Non assigné"

                historique = get_historique_maitre(id_affectation)

                historique_controls = []
                if historique:
                    historique_controls.append(
                        ft.Text("  🕘 Historique des maîtres :",
                                size=11, weight="bold", color="#6B7280")
                    )
                    for h in historique:
                        date_ch   = fmt_str(h[0])
                        anc_nom   = f"{(h[2] or '').strip()} {(h[1] or '').strip()}".strip()
                        nv_nom    = f"{(h[4] or '').strip()} {(h[3] or '').strip()}".strip()
                        anc_label = anc_nom if anc_nom else "Aucun"

                        historique_controls.append(
                            ft.Container(
                                content=ft.Row([
                                    ft.Icon(ft.icons.HISTORY, size=13, color="#9CA3AF"),
                                    ft.Text(
                                        f"{date_ch}  :  ",
                                        size=11, color="#9CA3AF",
                                    ),
                                    ft.Text(
                                        anc_label,
                                        size=11,
                                        color="#EF4444",  # ancien en rouge
                                        weight="bold",
                                    ),
                                    ft.Text(" → ", size=11, color="#9CA3AF"),
                                    ft.Text(
                                        nv_nom,
                                        size=11,
                                        color="#10B981",  # nouveau en vert
                                        weight="bold",
                                    ),
                                ], spacing=2),
                                bgcolor="#FAFAFA",
                                border_radius=6,
                                padding=ft.Padding(8, 4, 8, 4),
                                border=ft.border.all(1, "#E5E7EB"),
                            )
                        )

                controls.append(
                    ft.Container(
                        content=ft.Column([
                            ft.Row([
                                ft.Text("N° Aff. :", width=160),
                                ft.Text(str(r[1] or ""), weight="bold"),
                            ]),
                            ft.Row([ft.Text("Projet :",  width=160), ft.Text(r[2] or "")]),
                            ft.Row([ft.Text("Adresse :", width=160), ft.Text(r[3] or "")]),
                            ft.Row([ft.Text("Du :",      width=160), ft.Text(fmt(r[4]))]),
                            ft.Row([ft.Text("Au :",      width=160), ft.Text(fmt(r[5]))]),

                            ft.Row([
                                ft.Text("Maître actuel :", width=160),
                                ft.Row([
                                    ft.Icon(ft.icons.PERSON, size=14, color="#20398d"),
                                    ft.Text(
                                        maitre_actuel,
                                        size=12, weight="bold",
                                        color="#20398d" if (maitre_nom or maitre_prenom)
                                            else "#9CA3AF",
                                        italic=not (maitre_nom or maitre_prenom),
                                    ),
                                ], spacing=4),
                            ]),

                            *historique_controls,
                            ft.Row([
                            ft.Container(expand=True),
                            ft.ElevatedButton(
                                "✏️ Modifier",
                                bgcolor="#20398d",
                                color="#FFFFFF",
                                height=32,
                                on_click=lambda e, id_af=id_affectation: open_modifier_affectation_projet_dialog(
                                    page, id_af, on_refresh
                                )
                            ),
                        ], alignment=ft.MainAxisAlignment.END),

                        ], spacing=5),
                        bgcolor="#F0F9FF",
                        border_radius=8,
                        padding=12,
                        border=ft.border.all(1, "#BAE6FD"),
                    ),
                )

            return controls

        except Exception as e:
            print(f"Erreur affectations : {e}")
            return []
        finally:
            conn.close()
    def create_apprenti_card(page, apprenti, user_info, show_gui, on_logout, show_login):
        def on_click(e):
            show_apprenti_detail(page, apprenti, user_info, show_gui, on_logout, show_login,
                                open_affecter_moyen=open_affecter_moyen_dialog )

        statut = apprenti.get("statut_contrat", "")
        couleur = statut_color(statut)

        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Container(expand=True),
                    ft.Container(
                        content=ft.Text(statut, size=10, weight="bold", color="#FFFFFF"),
                        bgcolor=couleur,
                        border_radius=20,
                        padding=ft.Padding(8, 3, 8, 3),
                        visible=bool(statut),
                    ),
                ]),
                ft.Row([
                    ft.Container(
                        width=50, height=50,
                        border_radius=25,
                        clip_behavior=ft.ClipBehavior.ANTI_ALIAS,
                        bgcolor='E0E7FF',
                        border=ft.border.all(2, '#20398d'),
                        content=ft.Image(
                            src=apprenti.get('photo_path', 'assets/default_avatar.png'),
                            width=50, height=50,
                            fit=ft.ImageFit.COVER,
                            border_radius=25
                        )
                    ),
                    ft.Container(
                        expand=True,
                        content=ft.Column([
                            ft.Text(
                                f"{apprenti.get('code_app', '')} - "
                                f"{apprenti.get('prenom', '')} {apprenti.get('nom', '')}",
                                size=14, weight="bold", color="#20398d"
                            ),
                            ft.Row([
                                ft.Icon(ft.icons.EMAIL_OUTLINED, size=14, color="#6B7280"),
                                ft.Text(apprenti.get("mail", ""), size=11, color="#6B7280"),
                            ], spacing=6),
                            ft.Row([
                                ft.Icon(ft.icons.PHONE_ANDROID_OUTLINED, size=14, color="#6B7280"),
                                ft.Text(apprenti.get("telephone", ""), size=11, color="#6B7280"),
                            ], spacing=6),
                        ], spacing=3),
                        padding=10,
                    ),
                ], spacing=10, alignment=ft.MainAxisAlignment.START),
            ], spacing=4),
            width=300, padding=12,
            bgcolor="#FFFFFF", border_radius=10,
            border=ft.border.all(1, "#E5E7EB"),
            shadow=ft.BoxShadow(blur_radius=3, color="#0000000A"),
            on_click=on_click,
        )


    def create_apprenti_list_item(page, apprenti, user_info, show_gui, on_logout, show_login):
        def on_click(e):
            show_apprenti_detail(page, apprenti, user_info, show_gui, on_logout, show_login,
                                open_affecter_moyen=open_affecter_moyen_dialog)

        statut = apprenti.get("statut_contrat", "")
        couleur = statut_color(statut)

        return ft.Container(
            content=ft.Row([
                ft.Container(
                    content=ft.Image(
                        src=apprenti.get('photo_path', 'assets/default_avatar.png'),
                        width=50, height=50, fit=ft.ImageFit.COVER,
                    ),
                    width=50, height=50, border_radius=25, bgcolor="#E0E7FF",
                ),
                ft.Container(
                    expand=True,
                    content=ft.Column([
                        ft.Row([
                            ft.Text(
                                f"{apprenti.get('code_app', '')} - {apprenti.get('prenom', '')} {apprenti.get('nom', '')}",
                                size=13, weight="bold", color="#20398d", expand=True
                            ),
                            ft.Container(
                                content=ft.Text(statut, size=10, weight="bold", color="#FFFFFF"),
                                bgcolor=couleur,
                                border_radius=20,
                                padding=ft.Padding(8, 3, 8, 3),
                                visible=bool(statut),
                            ),
                        ], spacing=5),
                        ft.Row([
                            ft.Row([
                                ft.Icon(ft.icons.EMAIL_OUTLINED, size=14, color="#6B7280"),
                                ft.Text(apprenti.get("mail", ""), size=11, color="#6B7280"),
                            ], spacing=4),
                            ft.Row([
                                ft.Icon(ft.icons.PHONE_ANDROID, size=14, color="#6B7280"),
                                ft.Text(apprenti.get("telephone", ""), size=11, color="#6B7280"),
                            ], spacing=4, width=140),
                        ], spacing=10),
                    ], spacing=4),
                    padding=8,
                ),
            ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            padding=10,
            bgcolor="#FFFFFF", border_radius=8,
            border=ft.border.all(1, "#E5E7EB"),
            on_click=on_click,
        )
    def filter_apprentis():
        query = search_query["current"].lower()
        if not query:
            return apprentis_data
        return [
            a for a in apprentis_data
            if query in (a.get('nom') or '').lower()
            or query in (a.get('prenom') or '').lower()
            or query in (a.get('mail') or '').lower()
            or query in (a.get('telephone') or '').lower()
            or query in (a.get('sous_specialite') or '').lower()
            or query in (a.get('branche') or '').lower()
        ]
        
    def update_content_area():
        filtered_data = filter_apprentis()

        count = len(filtered_data)
        count_text.value = f"{count} apprenti{'s' if count != 1 else ''}"

        if not filtered_data:
            content_area.content = ft.Column([
                ft.Icon(ft.icons.SEARCH_OFF, size=80, color="#9CA3AF"),
                ft.Text("Aucun apprenti trouvé", size=18, color="#6B7280", weight="bold"),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            alignment=ft.MainAxisAlignment.CENTER)

        elif view_mode["current"] == "grid":
            grid_items = [
                create_apprenti_card(page, a, user_info, show_gui, on_logout, show_login)
                for a in filtered_data
            ]
            rows = []
            for i in range(0, len(grid_items), 3):
                rows.append(ft.Row(grid_items[i:i+3], spacing=20,
                                alignment=ft.MainAxisAlignment.START))
            content_area.content = ft.Column(rows, spacing=20,
                                            scroll=ft.ScrollMode.AUTO, expand=True)
        else:
            list_items = [
                create_apprenti_list_item(page, a, user_info, show_gui, on_logout, show_login)
                for a in filtered_data
            ]
            content_area.content = ft.Column(list_items, spacing=10,
                                            scroll=ft.ScrollMode.AUTO, expand=True)
        page.update()
    def toggle_view(e):
        view_mode["current"] = "list" if view_mode["current"] == "grid" else "grid"
        
        if view_mode["current"] == "grid":
            grid_btn.bgcolor = "#20398d"
            grid_btn.icon_color = "#FFFFFF"
            list_btn.bgcolor = "#E5E7EB"
            list_btn.icon_color = "#6B7280"
        else:
            grid_btn.bgcolor = "#E5E7EB"
            grid_btn.icon_color = "#6B7280"
            list_btn.bgcolor = "#20398d"
            list_btn.icon_color = "#FFFFFF"
        
        update_content_area()
    
    def on_search_change(e):
        search_query["current"] = e.control.value
        update_content_area()
    
    def on_add_apprenti(e):
        form = ApprentiForm(
            page,
            current_user=user_info,
            on_saved=return_to_list,
            on_close=return_to_list,
        )

        form.show()
        page.update()
    
    def toggle_profile_menu(e):
        show_profile_menu["current"] = not show_profile_menu["current"]
        profile_dropdown.visible = show_profile_menu["current"]
        page.update()

    search_field = ft.TextField(
        hint_text="Rechercher...",
        border=ft.InputBorder.NONE,
        expand=True,
        on_change=on_search_change,
    )
    
    search_bar = ft.Container(
        content=ft.Row([ft.Icon(ft.icons.SEARCH, color="#6B7280", size=22), search_field], spacing=10),
        bgcolor="#FFFFFF",
        border_radius=25,
        width=500,
        height=48,
        shadow=ft.BoxShadow(blur_radius=10, color="#0000000A"),
    )
    
    profile_dropdown = ft.Container(
        content=ft.Column(
            [
                ft.Container(
                    content=ft.Row([
                        ft.Icon(ft.icons.LOGOUT, size=16, color="#EF4444"),
                        ft.Text("Déconnexion", size=14, color="#EF4444", weight="w600"),
                    ], spacing=10),
                    padding=ft.Padding(12, 8, 12, 8),
                    on_click=lambda e: on_logout(),
                    border_radius=6,
                ),
            ],
            spacing=4,
        ),
        bgcolor="#FFFFFF",
        border_radius=12,
        padding=8,
        width=220,
        visible=False,
        right=20,
        top=70,
    )
    
    account_info = ft.Container(
        content=ft.Stack([
            ft.Row([
                ft.Column([
                    ft.Text(f"{user_info['prenom']} {user_info['nom']}", weight="bold", color="#1F2937", size=14),
                    ft.Text(user_info.get('email', ''), color="#6B7280", size=11),
                ], spacing=4, horizontal_alignment=ft.CrossAxisAlignment.END, tight=True),
                ft.Container(
                    content=ft.Text(f"{user_info['prenom'][0]}{user_info['nom'][0]}", size=16, weight="bold", color="#FFFFFF", text_align=ft.TextAlign.CENTER),
                    bgcolor="#20398d",
                    border_radius=20,
                    width=40,
                    height=40,
                    alignment=ft.Alignment(0, 0),
                    on_click=toggle_profile_menu,
                ),
            ], spacing=12, alignment=ft.MainAxisAlignment.END, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            profile_dropdown,
        ]),
        padding=ft.Padding(0, 0, 20, 0),
    )
    
    header = ft.Container(
        content=ft.Row([search_bar, account_info], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
        bgcolor="#FFFFFF",
        height=80,
        padding=ft.Padding(30, 0, 30, 0),
        shadow=ft.BoxShadow(blur_radius=4, color="#0000000D"),
    )
    
    grid_btn = ft.IconButton(icon=ft.icons.GRID_VIEW, icon_color="#FFFFFF", bgcolor="#20398d", on_click=toggle_view)
    list_btn = ft.IconButton(icon=ft.icons.VIEW_LIST, icon_color="#6B7280", bgcolor="#E5E7EB", on_click=toggle_view)
    
    count_text = ft.Text("0 apprenti", size=14, color="#6B7280")
    
    add_btn = ft.FilledButton(
        content=ft.Row([ft.Icon(ft.icons.ADD, size=18), ft.Text("Nouvel apprenti", size=14, weight="w600")], spacing=8),
        style=ft.ButtonStyle(bgcolor="#20398d", shape=ft.RoundedRectangleBorder(radius=8)),
        height=40,
        on_click=on_add_apprenti,
    )
    def on_fiche_globale_click(e):
        if selected_menu["type"] != "specialite" or selected_menu["id"] is None:
            page.snack_bar = ft.SnackBar(
                ft.Text("⚠️ Sélectionnez d'abord une spécialité dans le menu"),
                duration=3000)
            page.snack_bar.open = True
            page.update()
            return
        sp_id  = selected_menu["id"]
        sp_nom = next((sp[3] for sp in specialites if sp[0] == sp_id), "Inconnue")
        annee_field = ft.TextField(
            label="Année", width=150,
            value=str(datetime.now().year),
            border=ft.InputBorder.OUTLINE, border_radius=10,
        )
        def valider_annee(e):
            dlg.open = False
            page.update()
            try:
                annee = int(annee_field.value)
            except ValueError:
                page.snack_bar = ft.SnackBar(ft.Text("❌ Année invalide"))
                page.snack_bar.open = True
                page.update()
                return
            _lancer_generation_pdf(page, generer_fiche_globale, annee, sp_id, sp_nom)
        dlg = ft.AlertDialog(
            title=ft.Text("📊 Fiche globale", color="#20398d", weight="bold"),
            content=ft.Column([
                ft.Text(f"Spécialité : {sp_nom}", size=13, color="#6B7280"),
                annee_field,
            ], spacing=12, tight=True),
            actions=[
                ft.TextButton("Annuler",
                    on_click=lambda e: (setattr(dlg, 'open', False), page.update())),
                ft.ElevatedButton("Générer", bgcolor="#20398d", on_click=valider_annee),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        page.dialog = dlg
        dlg.open = True
        page.update()
 
    fiche_globale_btn = ft.FilledButton(
        content=ft.Row([
            ft.Icon(ft.icons.PICTURE_AS_PDF, size=18),
            ft.Text("Fiche globale", size=14, weight="w600"),
        ], spacing=8),
        style=ft.ButtonStyle(
            bgcolor="#7C3AED",
            shape=ft.RoundedRectangleBorder(radius=8)
        ),
        height=40,
        on_click=on_fiche_globale_click,
        tooltip="Génère la fiche globale de la spécialité sélectionnée",
    )
    
    tabs_bar = ft.Container(
        content=ft.Row(
            [
                ft.Row([ft.Icon(ft.icons.FILTER_LIST, size=20, color="#20398d"), count_text], spacing=15),
                ft.Row([add_btn, fiche_globale_btn , grid_btn, list_btn], spacing=10),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        ),
        bgcolor="#FFFFFF",
        padding=ft.Padding(25, 15, 25, 15),
    )
    
    content_area = ft.Container(
        expand=True,
        padding=20
    )    
    load_apprentis()

    
    build_sidebar()
    
    return ft.Row([
        sidebar,
        ft.VerticalDivider(width=1, color="#E5E7EB"),  
        ft.Column([
            header, tabs_bar, content_area
        ], expand=True, spacing=0),
    ], expand=True, spacing=0)
def main(page: ft.Page):
    def show_login():
        page.clean()
        page.add(login_page(page, show_gui))
        page.update()
    
    def show_gui(user_info):
        page.clean()
        page.add(gui_page(page, user_info, show_login, show_login, show_gui))
        page.update()
    
    show_login()
if __name__ == "__main__":
    ft.app(target=main)